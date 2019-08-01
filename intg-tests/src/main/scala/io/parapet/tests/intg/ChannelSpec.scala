package io.parapet.tests.intg

import cats.effect.{Concurrent, Timer}
import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.{Channel, Event, Process}
import io.parapet.tests.intg.ChannelSpec._
import io.parapet.testutils.{EventStore, IntegrationSpec, TestApp}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.util.{Success, Failure => SFailure}

abstract class ChannelSpec[F[_] : Concurrent : Timer : TestApp] extends FunSuite with IntegrationSpec[F] {

  import dsl._

  val ct: Concurrent[F] = implicitly[Concurrent[F]]

  test("channel") {

    val eventStore = new EventStore[F, Event]

    val numOfRequests = 5

    val server = Process[F](_ => {
      case Request(seq) => withSender(sender => Response(seq) ~> sender)
    })

    val client: Process[F] = new Process[F] {

      var seq = 0

      val ch = new Channel[F]()

      def sendRequest(request: Request): DslF[F, Unit] =
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

    runSync(eventStore.await(numOfRequests, run(ct.pure(Seq(client, server)))))
    eventStore.get(client.ref) shouldBe Seq(Response(0), Response(1), Response(2), Response(3), Response(4))

  }
}


object ChannelSpec {

  case class Request(seq: Int) extends Event

  case class Response(seq: Int) extends Event

}