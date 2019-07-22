package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Dsl.{DslF, WithDsl}
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.intg.ChannelSpec.{Request, Response}
import io.parapet.core.testutils.{EventStore, IntegrationSpec}
import io.parapet.core.{Channel, Event, Process}
import io.parapet.implicits._
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import scala.util.{Failure => SFailure, Success}

class ChannelSpec extends FunSuite with IntegrationSpec with WithDsl[IO] {

  import effectDsl._
  import flowDsl._


  test("channel") {

    val eventStore = new EventStore[Event]

    val numOfRequests = 5

    val server = new Process[IO] {
      override def handle: Receive = {
        case Request(seq) => reply(sender => Response(seq) ~> sender)
      }
    }

    val client: Process[IO] = new Process[IO] {
      var seq = 0

      val ch = new Channel[IO]()

      def sendRequest(request: Request): DslF[IO, Unit] =
        ch.send(request, server.selfRef, {
          case SFailure(err) => eval(throw err)
          case Success(res) => eval(eventStore.add(selfRef, res))
        })

      override def handle: Receive = {
        case Start => register(selfRef, ch) ++
          (0 until numOfRequests).map(i => Request(i)) ~> selfRef
        case Stop => empty
        case req: Request => sendRequest(req)
      }
    }

    val program = for {
      fiber <- run(empty, Array(client, server)).start
      _ <- eventStore.awaitSize(numOfRequests).guaranteeCase(_ => fiber.cancel)

    } yield ()
    program.unsafeRunSync()
    eventStore.get(client.selfRef) shouldBe Seq(Response(0), Response(1), Response(2), Response(3), Response(4))

  }
}


object ChannelSpec {

  case class Request(seq: Int) extends Event

  case class Response(seq: Int) extends Event

}