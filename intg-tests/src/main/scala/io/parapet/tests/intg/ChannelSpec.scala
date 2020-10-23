package io.parapet.tests.intg

import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Scheduler.SchedulerConfig
import io.parapet.core.{Channel, Event, Process, ProcessRef}
import io.parapet.tests.intg.ChannelSpec._
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.util.{Success, Failure => SFailure}

abstract class ChannelSpec[F[_]] extends FunSuite with IntegrationSpec[F] {

  import dsl._

  test("channel") {

    val eventStore = new EventStore[F, Event]

    val numOfRequests = 5

    val server = Process.builder[F](_ => {
      case Request(seq) => withSender(sender => Response(seq) ~> sender)
    }).name("server").ref(ProcessRef("server")).build

    val client: Process[F] = new Process[F] {
      override val name = "client"
      override val ref = ProcessRef("client")
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
            blocking {
              (0 until numOfRequests).map(i => sendRequest(Request(i))).reduce(_ ++ _)
            }
        case Stop => unit
      }
    }

    unsafeRun(eventStore.await(numOfRequests,
      createApp(ct.pure(Seq(client, server)), Option.empty, ParConfig(-1, SchedulerConfig(1))).run))
    eventStore.get(client.ref) shouldBe Seq(Response(0), Response(1), Response(2), Response(3), Response(4))

  }
}


object ChannelSpec {

  case class Request(seq: Int) extends Event

  case class Response(seq: Int) extends Event

}