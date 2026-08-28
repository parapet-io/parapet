package io.parapet.tests.intg

import io.parapet.Channel.{ChannelTimeoutException, UnexpectedChannelResponseException}
import io.parapet.Event.Start
import io.parapet.exceptions.EventHandlingException
import io.parapet.tests.intg.AskSpec._
import io.parapet.testutils.EventStore
import io.parapet.{Event, Process, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.duration.*
import scala.util.{Failure => SFailure, Success}

/** Covers [[io.parapet.Process.ask]]: the request/reply helper that runs each call on its own single-use
  * [[io.parapet.Channel]] registered as a child of the asking process.
  */
abstract class AskSpec[F[_]] extends AnyFunSuite with IntegrationSpec[F] {

  import dsl._

  test("ask completes with the reply") {
    val eventStore = new EventStore[F, Event]

    val server = new Process[F, Request] {
      override val ref: ProcessRef[Request] = ProcessRef("server")

      override def handle: Receive = { case Request(seq) =>
        reply(Response(seq))
      }
    }

    val client = new Process[F, Event] {
      override val ref: ProcessRef[Event] = ProcessRef("client")

      override def handle: Receive = { case Start =>
        ask[Response](Request(0), server.ref, 3.seconds).flatMap {
          case Success(response) => eval(eventStore.add(ref, response))
          case other             => eval(fail(s"expected a response, got $other"))
        }
      }
    }

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(client, server))).run))
    eventStore.get(client.ref) shouldBe Seq(Response(0))
  }

  test("consecutive asks each run on their own channel") {
    val eventStore = new EventStore[F, Event]

    val server = new Process[F, Request] {
      override val ref: ProcessRef[Request] = ProcessRef("server")

      override def handle: Receive = { case Request(seq) =>
        reply(Response(seq))
      }
    }

    val client = new Process[F, Event] {
      override val ref: ProcessRef[Event] = ProcessRef("client")

      def request(seq: Int): Program =
        ask[Response](Request(seq), server.ref, 3.seconds).flatMap {
          case Success(response) => eval(eventStore.add(ref, response))
          case other             => eval(fail(s"expected a response, got $other"))
        }

      override def handle: Receive = { case Start =>
        request(1) ++ request(2) ++ request(3)
      }
    }

    unsafeRun(eventStore.await(3, createApp(ct.pure(Seq(client, server))).run))
    eventStore.get(client.ref) shouldBe Seq(Response(1), Response(2), Response(3))
  }

  test("concurrent asks do not contend for a channel") {
    // A single shared channel rejects the second request with IllegalChannelStateException; because `ask` gives each
    // call its own channel, both requests can be in flight at once.
    val eventStore = new EventStore[F, Event]

    val server = new Process[F, Request] {
      override val ref: ProcessRef[Request] = ProcessRef("server")

      override def handle: Receive = { case Request(seq) =>
        delay(100.millis) ++ reply(Response(seq))
      }
    }

    val client = new Process[F, Event] {
      override val ref: ProcessRef[Event] = ProcessRef("client")

      def request(seq: Int): Program =
        ask[Response](Request(seq), server.ref, 3.seconds).flatMap {
          case Success(response) => eval(eventStore.add(ref, response))
          case other             => eval(fail(s"expected a response, got $other"))
        }

      override def handle: Receive = { case Start =>
        fork(request(1)).void ++ request(2)
      }
    }

    unsafeRun(eventStore.await(2, createApp(ct.pure(Seq(client, server))).run, timeout = 5.seconds))
    eventStore.get(client.ref).toSet shouldBe Set(Response(1), Response(2))
  }

  test("ask times out when the receiver does not reply") {
    val eventStore = new EventStore[F, Event]

    val silent = new Process[F, Request] {
      override val ref: ProcessRef[Request] = ProcessRef("silent")

      override def handle: Receive = { case Request(_) =>
        unit
      }
    }

    val client = new Process[F, Event] {
      override val ref: ProcessRef[Event] = ProcessRef("client")

      override def handle: Receive = { case Start =>
        ask[Response](Request(0), silent.ref, 25.millis).flatMap {
          case SFailure(ChannelTimeoutException(_, _)) => eval(eventStore.add(ref, TimedOut))
          case other                                   => eval(fail(s"expected timeout, got $other"))
        }
      }
    }

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(client, silent))).run))
    eventStore.get(client.ref) shouldBe Seq(TimedOut)
  }

  test("ask keeps working after a timeout") {
    val eventStore = new EventStore[F, Event]

    val silent = new Process[F, Request] {
      override val ref: ProcessRef[Request] = ProcessRef("silent")

      override def handle: Receive = { case Request(_) =>
        unit
      }
    }

    val server = new Process[F, Request] {
      override val ref: ProcessRef[Request] = ProcessRef("server")

      override def handle: Receive = { case Request(seq) =>
        reply(Response(seq))
      }
    }

    val client = new Process[F, Event] {
      override val ref: ProcessRef[Event] = ProcessRef("client")

      override def handle: Receive = { case Start =>
        ask[Response](Request(0), silent.ref, 25.millis).flatMap {
          case SFailure(ChannelTimeoutException(_, _)) => eval(eventStore.add(ref, TimedOut))
          case other                                   => eval(fail(s"expected timeout, got $other"))
        } ++ ask[Response](Request(1), server.ref, 3.seconds).flatMap {
          case Success(response) => eval(eventStore.add(ref, response))
          case other             => eval(fail(s"expected a response, got $other"))
        }
      }
    }

    unsafeRun(eventStore.await(2, createApp(ct.pure(Seq(client, silent, server))).run, timeout = 5.seconds))
    eventStore.get(client.ref) shouldBe Seq(TimedOut, Response(1))
  }

  test("ask fails when the reply does not match the expected type") {
    val eventStore = new EventStore[F, Event]

    val server = new Process[F, Request] {
      override val ref: ProcessRef[Request] = ProcessRef("server")

      override def handle: Receive = { case Request(_) =>
        reply(WrongResponse)
      }
    }

    val client = new Process[F, Event] {
      override val ref: ProcessRef[Event] = ProcessRef("client")

      override def handle: Receive = { case Start =>
        ask[Response](Request(0), server.ref, 3.seconds).flatMap {
          case SFailure(_: UnexpectedChannelResponseException) => eval(eventStore.add(ref, WrongReply))
          case other => eval(fail(s"expected an unexpected-response failure, got $other"))
        }
      }
    }

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(client, server))).run))
    eventStore.get(client.ref) shouldBe Seq(WrongReply)
  }

  test("ask surfaces a failure raised by the receiver") {
    val eventStore = new EventStore[F, Event]

    val server = new Process[F, Request] {
      override val ref: ProcessRef[Request] = ProcessRef("server")

      override def handle: Receive = { case Request(_) =>
        eval(throw new RuntimeException("505"))
      }
    }

    val client = new Process[F, Event] {
      override val ref: ProcessRef[Event] = ProcessRef("client")

      override def handle: Receive = { case Start =>
        ask[Response](Request(0), server.ref, 3.seconds).flatMap {
          case SFailure(EventHandlingException(_, cause)) if cause.getMessage == "505" =>
            eval(eventStore.add(ref, ReceiverFailed))
          case other => eval(fail(s"expected a receiver failure, got $other"))
        }
      }
    }

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(client, server))).run))
    eventStore.get(client.ref) shouldBe Seq(ReceiverFailed)
  }

}

object AskSpec {

  case class Request(seq: Int) extends Event

  case class Response(seq: Int) extends Event

  case object WrongResponse extends Event

  case object TimedOut extends Event

  case object WrongReply extends Event

  case object ReceiverFailed extends Event

}
