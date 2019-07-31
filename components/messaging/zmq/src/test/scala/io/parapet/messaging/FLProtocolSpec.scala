package io.parapet.messaging

import java.net.ServerSocket

import cats.effect.{Concurrent, IO}
import com.typesafe.scalalogging.StrictLogging
import io.parapet.messaging.FLProtocolSpec.{FLTestClient, TestRequest, TestResponse}
import io.parapet.messaging.api.MessagingApi.Success
import io.parapet.messaging.api.{FLProtocolApi, HeartbeatAPI, MessagingApi, ServerAPI}
import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.{Channel, Encoder, Event, Process, ProcessRef}
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.concurrent.duration._

class FLProtocolSpec extends FunSuite with IntegrationSpec with StrictLogging {

  import dsl._

  test("freelance pattern") {

    val eventStore = new EventStore[Event]

    val encoder = Encoder.json(List(

      // Messaging Api
      classOf[MessagingApi.Request],
      classOf[MessagingApi.Success],
      classOf[MessagingApi.Failure],

      // Test Api
      classOf[TestRequest],
      classOf[TestResponse],

      // Heartbeat Api
      HeartbeatAPI.Ping.getClass,
      HeartbeatAPI.Pong.getClass,

      // FL Api
      classOf[FLProtocolApi.Connect],

      // Server Api
      classOf[ServerAPI.Envelope]
    ))


    val availableServerPort = new ServerSocket(0).getLocalPort

    val flprotocol: Process[IO] = new FLProtocol[IO](encoder)

    val service = Process[IO](_ => {
      case ServerAPI.Envelope(id, TestRequest(body)) =>
        withSender(sender => {
          ServerAPI.Envelope(id, TestResponse("server-" + body)) ~> sender
        })
    })

    val client: Process[IO] = new FLTestClient(flprotocol.ref,
      List("tcp://localhost:4444", "tcp://localhost:5555", s"tcp://localhost:$availableServerPort"),
      eventStore)

    val server: Process[IO] = ZmqAsyncServer(s"tcp://*:$availableServerPort",
      service.ref, encoder, 1, s"tcp://localhost:$availableServerPort")

    val processes: Seq[Process[IO]] = Seq(flprotocol, client, service, server)


    eventStore.awaitSize(5, run(processes)).unsafeRunSync()

    // at least two last request must be successfuly delivered
    eventStore.get(client.ref).slice(3, 5) shouldBe
      Seq(Success(TestResponse("server-3")), Success(TestResponse("server-4")))

  }


}

object FLProtocolSpec {

  case class TestRequest(body: String) extends Event

  case class TestResponse(body: String) extends Event

  class FLTestClient[F[_] : Concurrent](flprotocol: ProcessRef,
                                        servers: List[String],
                                        eventStore: EventStore[Event]) extends Process[F] {

    import dsl._

    val ch = new Channel[F]()

    override def handle: Receive = {
      case Start =>
        register(ref, ch) ++
          servers.map(endpoint => FLProtocolApi.Connect(endpoint)) ~> flprotocol ++
          delay(1.second, generateRequests(5, ch))
      case Stop => unit
    }

    def generateRequests(n: Int, ch: Channel[F]): DslF[F, Unit] = {
      (0 until n).map { i =>
        ch.send(MessagingApi.Request(TestRequest(i.toString)), flprotocol, {
          case scala.util.Success(res) => eval(eventStore.add(ref, res))
          case scala.util.Failure(err) => eval(println(err))
        })
      }.fold(unit)(_ ++ _)
    }
  }

}
