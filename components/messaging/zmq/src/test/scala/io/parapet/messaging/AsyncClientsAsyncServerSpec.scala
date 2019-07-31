package io.parapet.messaging

import java.net.ServerSocket

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import io.parapet.messaging.AsyncClientsAsyncServerSpec._
import io.parapet.messaging.api.MessagingApi.{Failure, Request, Response, Success}
import io.parapet.messaging.api.ServerAPI.Envelope
import io.parapet.core.Event.Start
import io.parapet.core.{Encoder, Event, Process, ProcessRef}
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.FunSuite
import org.scalatest.Matchers.{empty => _, _}

class AsyncClientsAsyncServerSpec extends FunSuite with IntegrationSpec with StrictLogging {

  import dsl._

  private val encoder = Encoder.json(
    List(
      // Messaging Api
      classOf[Request],
      classOf[Success],
      classOf[Failure],
      // Test Api
      classOf[TestRequest],
      classOf[TestResponse])
  )

  test("many sync clients with async server") {

    val asyncService: Process[IO] = new Process[IO] {
      override val name = "worker"
      override val ref = ProcessRef("worker")

      override def handle: Receive = {
        case Envelope(requestId, TestRequest(id)) =>
          fork(withSender(sender => Envelope(requestId, TestResponse(id)) ~> sender))
      }
    }

    val specs = Seq(
      Spec(
        numOfWorkers = 5,
        numberOfClients = 5,
        numberOfEventsPerClient = 10,
        service = asyncService
      )
    )

    specs.foreach(spec => runSpec(spec))

  }

  case class Spec(
                   numOfWorkers: Int,
                   numberOfClients: Int,
                   numberOfEventsPerClient: Int,
                   service: Process[IO])

  def runSpec(spec: Spec): Unit = {

    logger.info(s"run spec: $spec")
    val eventStore = new EventStore[Response]


    val port = new ServerSocket(0).getLocalPort

    val server: Process[IO] = ZmqAsyncServer(s"tcp://*:$port", spec.service.ref, encoder, spec.numOfWorkers)


    val clients: Map[Process[IO], Seq[TestRequest]] = (0 until spec.numberOfClients).map { i =>
      val clientName = s"client-$i"
      val requests = (0 until spec.numberOfEventsPerClient).map(i => TestRequest(s"$clientName-$i"))
      createSyncClient(port, eventStore, clientName, requests) -> requests
    }.toMap

    val processes: Seq[Process[IO]] = (clients.keys ++ Seq(server, spec.service)).toSeq
    eventStore.awaitSize(spec.numberOfClients * spec.numberOfEventsPerClient, run(processes)).unsafeRunSync
    eventStore.print()
    eventStore.size shouldBe spec.numberOfClients * spec.numberOfEventsPerClient

    clients.foreach {
      case (p, requests) => eventStore.get(p.ref).map {
        case Success(TestResponse(data)) => data
      } shouldBe requests.map(_.data)
    }

  }

  def createSyncClient(
                        port: Int,
                        eventStore: EventStore[Response],
                        cltName: String,
                        events: Seq[Event]): Process[IO] = new Process[IO] {
    override val name: String = cltName
    override val ref: ProcessRef = ProcessRef(cltName)

    override def handle: Receive = {
      case Start =>
        evalWith(ZmqAsyncClient[IO](s"tcp://localhost:$port", encoder)) { zmqClient =>
          register(ref, zmqClient) ++ events.map(Request) ~> zmqClient.ref
        }

      case res: Response => eval(eventStore.add(ref, res))
    }
  }
}

object AsyncClientsAsyncServerSpec {

  case class TestRequest(data: String) extends Event

  case class TestResponse(data: String) extends Event

}
