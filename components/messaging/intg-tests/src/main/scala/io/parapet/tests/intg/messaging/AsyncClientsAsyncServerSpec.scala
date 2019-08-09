package io.parapet.tests.intg.messaging

import java.net.ServerSocket

import cats.syntax.flatMap._
import cats.syntax.functor._
import io.parapet.core.Event.Start
import io.parapet.core.{Encoder, Event, Process, ProcessRef}
import io.parapet.messaging.api.MessagingApi.{Failure, Request, Response, Success}
import io.parapet.messaging.api.ServerAPI.Envelope
import io.parapet.messaging.{ZmqAsyncClient, ZmqAsyncServer}
import io.parapet.tests.intg.messaging.AsyncClientsAsyncServerSpec._
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

abstract class AsyncClientsAsyncServerSpec[F[_]] extends FunSuite with IntegrationSpec[F] {

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

    val asyncService: Process[F] = new Process[F] {
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
                   service: Process[F])

  def runSpec(spec: Spec): Unit = {

    logger.info(s"run spec: $spec")
    val eventStore = new EventStore[F, Response]


    val port = new ServerSocket(0).getLocalPort

    val processes = for {
      asyncServer <- ct.pure(ZmqAsyncServer[F](s"tcp://*:$port", spec.service.ref, encoder, spec.numOfWorkers))
      asyncClient <- ZmqAsyncClient[F](s"tcp://localhost:$port", encoder)
      clients <- ct.pure((0 until spec.numberOfClients).map(cliId =>
        createClient(cliId, eventStore, asyncClient,
          (0 until spec.numberOfEventsPerClient).map(i => TestRequest(s"cli-$cliId-$i")))))

    } yield Seq(asyncClient, asyncServer, spec.service) ++ clients


    unsafeRun(eventStore.await(spec.numberOfClients * spec.numberOfEventsPerClient, createApp(processes).run))
    eventStore.print()
    eventStore.size shouldBe spec.numberOfClients * spec.numberOfEventsPerClient

    (0 until spec.numberOfClients).foreach { cliId =>

      val expectedResponses = (0 until spec.numberOfEventsPerClient).map(i =>
        Success(TestResponse(s"cli-$cliId-$i")))

      eventStore.get(ProcessRef(s"cli-$cliId")) shouldBe expectedResponses
    }

  }

  def createClient(id: Int,
                   eventStore: EventStore[F, Response],
                   asyncClient: Process[F],
                   events: Seq[Event]): Process[F] = new Process[F] {
    override val name: String = s"cli-$id"
    override val ref: ProcessRef = ProcessRef(s"cli-$id")

    override def handle: Receive = {
      case Start =>
        events.map(Request) ~> asyncClient
      case res: Response => eval(eventStore.add(ref, res))
    }
  }
}

object AsyncClientsAsyncServerSpec {

  case class TestRequest(data: String) extends Event

  case class TestResponse(data: String) extends Event

}