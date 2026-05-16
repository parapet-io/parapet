package io.parapet.tests.intg

import io.parapet.core.Dsl.DslF
import io.parapet.core.Events.{Start, Stop}
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Scheduler.SchedulerConfig
import io.parapet.core.exceptions.EventHandlingException
import io.parapet.core.{Channel, Process}
import io.parapet.core.Channel.{
  ChannelTimeoutException,
  IllegalChannelStateException,
  UnexpectedChannelResponseException
}
import io.parapet.tests.intg.ChannelSpec._
import io.parapet.testutils.EventStore
import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.duration.*
import scala.util.{Failure => SFailure, Success}

abstract class ChannelSpec[F[_]] extends AnyFunSuite with IntegrationSpec[F] {

  import dsl._

  test("channel can be reused for sequential request/reply dialogs") {

    val eventStore = new EventStore[F, Event]

    val numOfRequests = 5

    val server = new Process[F, Event, Event] {
      override val name = "server"
      override val ref  = ProcessRef("server")

      override def handle: Receive = { case Request(seq) =>
        reply(Response(seq))
      }
    }

    val client: Process[F, Event, Event] = new Process[F, Event, Event] {
      override val name = "client"
      override val ref  = ProcessRef("client")
      var seq           = 0

      val ch = new Channel[F, Request, Response]()

      def sendRequest(request: Request): DslF[F, Unit] =
        ch.send(request, server.ref).flatMap {
          case SFailure(err) => eval(throw err)
          case Success(res)  => eval(eventStore.add(ref, res))
        }

      override def handle: Receive = {
        case Start =>
          register(ref, ch) ++
            offload {
              (0 until numOfRequests).map(i => sendRequest(Request(i))).reduce(_ ++ _)
            }
        case Stop => unit
      }
    }

    unsafeRun(
      eventStore.await(
        numOfRequests,
        createApp(ct.pure(Seq(client, server)), Option.empty, ParConfig(-1, SchedulerConfig(1))).run
      )
    )
    eventStore.get(client.ref) shouldBe Seq(Response(0), Response(1), Response(2), Response(3), Response(4))

  }

  test("channel uses its ref when sending event to target") {
    val eventStore = new EventStore[F, Event]
    val clientRef  = ProcessRef("client")
    val serverRef  = ProcessRef("server")

    val ch = Channel[F, Request, Response]

    val server = new Process[F, Event, Event] {
      override val ref: ProcessRef[Event] = serverRef

      override def handle: Receive = { case req @ Request(seq) =>
        eval(eventStore.add(ch.ref, req)) ++ reply(Response(seq + 1))
      }
    }

    val client = new Process[F, Event, Event] {
      override val ref: ProcessRef[Event] = clientRef

      override def handle: Receive = { case Start =>
        flow {
          register(ref, ch) ++ ch.send(Request(0), serverRef).flatMap {
            case Success(response) => eval(eventStore.add(ref, response))
            case SFailure(err)     => eval(throw err)
          } ++ halt(ch.ref)
        }
      }

    }
    unsafeRun(eventStore.await(2, createApp(ct.pure(Seq(client, server))).run))
    eventStore.get(ch.ref) shouldBe Seq(Request(0))
    eventStore.get(client.ref) shouldBe Seq(Response(1))
  }

  test("channel times out when the receiver does not reply") {
    val eventStore = new EventStore[F, Event]
    val clientRef  = ProcessRef("client")

    val ch = Channel[F, Request, Response]

    val server = new Process[F, Request, Nothing] {
      override val ref: ProcessRef[Request] = ProcessRef("server")

      override def handle: Receive = { case Request(_) =>
        unit
      }
    }

    val client = new Process[F, Event, Event] {
      override val ref: ProcessRef[Event] = clientRef

      override def handle: Receive = { case Start =>
        register(ref, ch) ++ ch.send(Request(0), server.ref, 25.millis).flatMap {
          case SFailure(ChannelTimeoutException(_, _)) => eval(eventStore.add(ref, TimedOut))
          case other                                   => eval(fail(s"expected timeout, got $other"))
        }
      }
    }

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(client, server))).run))
    eventStore.get(client.ref) shouldBe Seq(TimedOut)
  }

  test("channel fails when the reply does not match the expected output type") {
    val eventStore = new EventStore[F, Event]
    val clientRef  = ProcessRef("client")

    val ch = Channel[F, Request, Response]

    val server = new Process[F, Request, WrongResponse.type] {
      override val ref: ProcessRef[Request] = ProcessRef("server")

      override def handle: Receive = { case Request(_) =>
        reply(WrongResponse)
      }
    }

    val client = new Process[F, Event, Event] {
      override val ref: ProcessRef[Event] = clientRef

      override def handle: Receive = { case Start =>
        register(ref, ch) ++ ch.send(Request(0), server.ref).flatMap {
          case SFailure(_: UnexpectedChannelResponseException) => eval(eventStore.add(ref, WrongReply))
          case other => eval(fail(s"expected unexpected response, got $other"))
        }
      }
    }

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(client, server))).run))
    eventStore.get(client.ref) shouldBe Seq(WrongReply)
  }

  test("channel rejects a second request while one is in flight") {
    val eventStore = new EventStore[F, Event]
    val clientRef  = ProcessRef("client")

    val ch = Channel[F, Request, Response]

    val server = new Process[F, Request, Response] {
      override val ref: ProcessRef[Request] = ProcessRef("server")

      override def handle: Receive = { case Request(seq) =>
        delay(250.millis) ++ reply(Response(seq))
      }
    }

    val client = new Process[F, Event, Event] {
      override val ref: ProcessRef[Event] = clientRef

      override def handle: Receive = { case Start =>
        register(ref, ch) ++
          fork {
            ch.send(Request(1), server.ref, 1.second).flatMap {
              case Success(response) => eval(eventStore.add(ref, response))
              case other             => eval(fail(s"expected first request to complete, got $other"))
            }
          }.void ++
          delay(50.millis) ++
          ch.send(Request(2), server.ref, 100.millis).flatMap {
            case SFailure(_: IllegalChannelStateException) => eval(eventStore.add(ref, ConcurrentRejected))
            case other => eval(fail(s"expected concurrent request rejection, got $other"))
          }
      }
    }

    unsafeRun(eventStore.await(2, createApp(ct.pure(Seq(client, server))).run, timeout = 5.seconds))
    eventStore.get(client.ref).toSet shouldBe Set(Response(1), ConcurrentRejected)
  }

  test("channel can be reused after a timeout") {
    val eventStore = new EventStore[F, Event]
    val clientRef  = ProcessRef("client")

    val ch = Channel[F, Request, Response]

    val silent = new Process[F, Request, Nothing] {
      override val ref: ProcessRef[Request] = ProcessRef("silent")

      override def handle: Receive = { case Request(_) =>
        unit
      }
    }

    val server = new Process[F, Request, Response] {
      override val ref: ProcessRef[Request] = ProcessRef("server")

      override def handle: Receive = { case Request(seq) =>
        reply(Response(seq))
      }
    }

    val client = new Process[F, Event, Event] {
      override val ref: ProcessRef[Event] = clientRef

      override def handle: Receive = { case Start =>
        register(ref, ch) ++
          ch.send(Request(0), silent.ref, 25.millis).flatMap {
            case SFailure(ChannelTimeoutException(_, _)) => eval(eventStore.add(ref, TimedOut))
            case other                                   => eval(fail(s"expected timeout, got $other"))
          } ++
          ch.send(Request(2), server.ref).flatMap {
            case Success(response) => eval(eventStore.add(ref, response))
            case other             => eval(fail(s"expected reuse after timeout to complete, got $other"))
          }
      }
    }

    unsafeRun(eventStore.await(2, createApp(ct.pure(Seq(client, silent, server))).run, timeout = 5.seconds))
    eventStore.get(client.ref) shouldBe Seq(TimedOut, Response(2))
  }

  test("channel returns receiver handler failures") {
    val eventStore = new EventStore[F, Event]
    val clientRef  = ProcessRef("client")

    val ch = Channel[F, Request, Response]

    val server = new Process[F, Request, Nothing] {
      override val ref: ProcessRef[Request] = ProcessRef("server")

      override def handle: Receive = { case Request(_) =>
        eval(throw new RuntimeException("505"))
      }
    }

    val client = new Process[F, Event, Event] {
      override val ref: ProcessRef[Event] = clientRef

      override def handle: Receive = { case Start =>
        register(ref, ch) ++ ch.send(Request(0), server.ref).flatMap {
          case SFailure(EventHandlingException(_, cause)) if cause.getMessage == "505" =>
            eval(eventStore.add(ref, ReceiverFailed))
          case other => eval(fail(s"expected receiver failure, got $other"))
        }
      }
    }

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(client, server))).run))
    eventStore.get(client.ref) shouldBe Seq(ReceiverFailed)
  }

  test("channel rejects a response from a process other than the active receiver") {
    val eventStore = new EventStore[F, Event]
    val clientRef  = ProcessRef("client")

    val ch = Channel[F, Request, Response]

    val server = new Process[F, Request, Nothing] {
      override val ref: ProcessRef[Request] = ProcessRef("server")

      override def handle: Receive = { case Request(_) =>
        unit
      }
    }

    val client = new Process[F, Event, Event] {
      override val ref: ProcessRef[Event] = clientRef

      override def handle: Receive = { case Start =>
        register(ref, ch) ++
          fork {
            ch.send(Request(1), server.ref, 1.second).flatMap {
              case SFailure(_: UnexpectedChannelResponseException) => eval(eventStore.add(ref, WrongSender))
              case other => eval(fail(s"expected wrong sender rejection, got $other"))
            }
          }.void ++
          delay(50.millis) ++
          Response(99) ~> ch.ref
      }
    }

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(client, server))).run, timeout = 5.seconds))
    eventStore.get(client.ref) shouldBe Seq(WrongSender)
  }

}

object ChannelSpec {

  case class Request(seq: Int) extends Event

  case class Response(seq: Int) extends Event

  case object WrongResponse extends Event

  case object TimedOut extends Event

  case object WrongReply extends Event

  case object ConcurrentRejected extends Event

  case object ReceiverFailed extends Event

  case object WrongSender extends Event

}
