package io.parapet.tests.intg.messaging

import io.parapet.core.Event.Start
import io.parapet.core.{Encoder, Event, Process, ProcessRef}
import io.parapet.messaging.api.MessagingApi.{Failure, Request, Response, Success}
import io.parapet.messaging.{ZmqSyncClient, ZmqSyncServer}
import io.parapet.tests.intg.messaging.SyncClientSyncServerSpec._
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.Matchers._

abstract class SyncClientSyncServerSpec[F[_]] extends BasicZMQSpec with IntegrationSpec[F] {

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
    val eventStore = new EventStore[F, Response]

    val port = 5555

    val worker: Process[F] = new Process[F] {
      override val name = "worker"
      override val ref = ProcessRef("worker")

      override def handle: Receive = {
        case TestRequest(id) => withSender(sender => TestResponse(id) ~> sender)
      }
    }

    val server: Process[F] = ZmqSyncServer(s"tcp://*:$port", worker.ref, encoder)
    val zmqClient: Process[F] = ZmqSyncClient(s"tcp://localhost:$port", encoder)

    val clients: Map[Process[F], Seq[TestRequest]] = (0 until numberOfClients).map { i =>
      val clientName = s"client-$i"
      val requests = (0 until numberOfEventsPerClient).map(i => TestRequest(s"$clientName-$i"))
      createClient(eventStore, ProcessRef(clientName), requests, zmqClient.ref) -> requests
    }.toMap

    val processes: Seq[Process[F]] = Seq(zmqClient, server, worker) ++ clients.keys.toSeq
    unsafeRun(eventStore.await(numberOfClients * numberOfEventsPerClient, createApp(ct.pure(processes)).run))


    eventStore.size shouldBe numberOfClients * numberOfEventsPerClient

    clients.foreach {
      case (p, requests) => eventStore.get(p.ref).map {
        case Success(TestResponse(data)) => data
      } shouldBe requests.map(_.data)
    }

  }


}

object SyncClientSyncServerSpec {

  def createClient[F[_]](eventStore: EventStore[F, Response], _ref: ProcessRef,
                         events: Seq[TestRequest],
                         zmqClient: ProcessRef): Process[F] = new Process[F] {

    import dsl._

    override val ref: ProcessRef = _ref

    override def handle: Receive = {
      case Start => events.map(Request(_)) ~> zmqClient
      case rep: Response => eval(eventStore.add(ref, rep))
    }
  }

  case class TestRequest(data: String) extends Event

  case class TestResponse(data: String) extends Event

}
