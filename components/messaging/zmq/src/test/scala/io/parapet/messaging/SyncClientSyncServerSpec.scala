package io.parapet.messaging

import java.net.ServerSocket

import cats.effect.IO
import io.parapet.core.Event.Start
import io.parapet.core.{Encoder, Event, Process, ProcessRef}
import io.parapet.messaging.SyncClientSyncServerSpec._
import io.parapet.messaging.api.MessagingApi.{Failure, Request, Response, Success}
import io.parapet.testutils.{BasicCatsIOSpec, EventStore}
import org.scalatest.FunSuite
import org.scalatest.Matchers.{empty => _, _}

class SyncClientSyncServerSpec extends FunSuite with BasicCatsIOSpec {

  import dsl._

  private val encoder =
    Encoder.json(
      List(
        // Messaging Api
        classOf[Request],
        classOf[Success],
        classOf[Failure],
        // Test Api
        classOf[TestRequest],
        classOf[TestResponse]
      )
    )

  test("sync client server") {

    val numberOfClients = 5
    val numberOfEventsPerClient = 10
    val eventStore = new EventStore[IO, Response]

    val port = new ServerSocket(0).getLocalPort

    val worker: Process[IO] = new Process[IO] {
      override val name = "worker"
      override val ref = ProcessRef("worker")

      override def handle: Receive = {
        case TestRequest(id) => withSender(sender => TestResponse(id) ~> sender)
      }
    }

    val server: Process[IO] = ZmqSyncServer(s"tcp://*:$port", worker.ref, encoder)
    val zmqClient: Process[IO] = ZmqSyncClient(s"tcp://localhost:$port", encoder)

    val clients: Map[Process[IO], Seq[TestRequest]] = (0 until numberOfClients).map { i =>
      val clientName = s"client-$i"
      val requests = (0 until numberOfEventsPerClient).map(i => TestRequest(s"$clientName-$i"))
      createClient(eventStore, ProcessRef(clientName), requests, zmqClient.ref) -> requests
    }.toMap

    val processes: Seq[Process[IO]] = Seq(zmqClient, server, worker) ++ clients.keys.toSeq
    unsafeRun(eventStore.await(numberOfClients * numberOfEventsPerClient, createApp(ct.pure(processes)).run))


    eventStore.size shouldBe numberOfClients * numberOfEventsPerClient

    clients.foreach {
      case (p, requests) => eventStore.get(p.ref).map {
        case Success(TestResponse(data)) => data
      } shouldBe requests.map(_.data)
    }

  }

  def createClient(eventStore: EventStore[IO, Response], _ref: ProcessRef, events: Seq[TestRequest], zmqClient: ProcessRef): Process[IO] = new Process[IO] {
    override val ref: ProcessRef = _ref

    override def handle: Receive = {
      case Start => events.map(Request(_)) ~> zmqClient
      case rep: Response => eval(eventStore.add(ref, rep))
    }
  }
}

object SyncClientSyncServerSpec {

  case class TestRequest(data: String) extends Event

  case class TestResponse(data: String) extends Event

}
