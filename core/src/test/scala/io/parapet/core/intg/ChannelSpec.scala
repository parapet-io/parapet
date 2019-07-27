package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.intg.ChannelSpec.{Request, Response}
import io.parapet.core.testutils.{EventStore, IntegrationSpec}
import io.parapet.core.{Channel, Event, Process}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.util.{Success, Failure => SFailure}

class ChannelSpec extends FunSuite with IntegrationSpec {

  import dsl._


  test("channel") {

    val eventStore = new EventStore[Event]

    val numOfRequests = 5

    val server = Process[IO](_ => {
      case Request(seq) => withSender(sender => Response(seq) ~> sender)
    })

    val client: Process[IO] = new Process[IO] {

      var seq = 0

      val ch = new Channel[IO]()

      def sendRequest(request: Request): DslF[IO, Unit] =
        ch.send(request, server.ref, {
          case SFailure(err) => eval(throw err)
          case Success(res) => eval(eventStore.add(ref, res))
        })

      override def handle: Receive = {
        case Start =>
          register(ref, ch) ++
            (0 until numOfRequests).map(i => sendRequest(Request(i))).reduce(_ ++ _)
        case Stop => unit
      }
    }

    eventStore.awaitSize(numOfRequests, run(Seq(client, server))).unsafeRunSync()
    eventStore.get(client.ref) shouldBe Seq(Response(0), Response(1), Response(2), Response(3), Response(4))

  }
}


object ChannelSpec {

  case class Request(seq: Int) extends Event

  case class Response(seq: Int) extends Event

}