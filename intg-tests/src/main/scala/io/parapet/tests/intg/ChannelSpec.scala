package io.parapet.tests.intg

import cats.effect.{Concurrent, IO}
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events.{Start, Stop}
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Scheduler.SchedulerConfig
import io.parapet.core.{Channel, Process}
import io.parapet.tests.intg.ChannelSpec._
import io.parapet.testutils.{EventStore, IntegrationSpec}
import io.parapet.{Event, ProcessRef}
import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.util.{Success, Failure => SFailure}

@Ignore
abstract class ChannelSpec[F[_]] extends AnyFunSuite with IntegrationSpec[F] {

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

  test("channel uses its ref when sending event to target") {
    val eventStore = new EventStore[F, Event]
    val clientRef = ProcessRef("client")
    val serverRef = ProcessRef("server")

    val ch = Channel[F]

    val server = new Process[F] {
      override def handle: Receive = {
        case req@Request(seq) => withSender { sender =>
          eval(eventStore.add(ch.ref, req)) ++
            Response(seq + 1) ~> sender
        }
      }
    }

    val client = new Process[F] {
      override val ref: ProcessRef = clientRef

      override def handle: Receive = {
        case Start => flow {
          register(ref) ++ ch.send(Request(0), serverRef).flatMap {
            case Success(response) => eval(eventStore.add(ref, response))
          } ++ halt(ch.ref)
        }
      }

    }
    unsafeRun(eventStore.await(2, createApp(ct.pure(Seq(client, server))).run))
    eventStore.get(ch.ref) shouldBe Seq(Request(0))
    eventStore.get(client.ref) shouldBe Seq(Response(1))
  }

}


object ChannelSpec {

  case class Request(seq: Int) extends Event

  case class Response(seq: Int) extends Event

}