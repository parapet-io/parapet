package io.parapet.core

import io.parapet.core.Dsl.DslF
import io.parapet.core.Events.{Failure, Start, Stop}
import io.parapet.effect.{Deferred, Effect}
import io.parapet.effect.Monad.*
import io.parapet.{Event, ProcessRef}

import java.util.concurrent.atomic.AtomicInteger
import scala.util.Try

/** A request/response helper process for synchronous-style interaction inside the asynchronous parapet runtime.
  *
  * A `Channel` is a small state machine: it accepts one [[Channel.Request]], forwards the embedded event to the target
  * process, and completes the request's [[Deferred]] with the next event delivered back to it. While a request is in
  * flight the channel rejects additional requests; this enforces a strict "one outstanding call" discipline.
  *
  * Callers typically construct a channel, register it under their own process, and use [[send]] to obtain a
  * `Try[Event]` representing the eventual reply (or a transport failure).
  *
  * @param ref
  *   optional fixed reference; defaults to a fresh UUID.
  */
class Channel[F[_]](override val ref: ProcessRef = ProcessRef.jdkUUIDRef)(using Effect[F]) extends Process[F]:
  import Channel.*
  import dsl.*

  private val runtimeDsl = summon[Dsl.RuntimeOps.Aux[F]]
  import runtimeDsl.*

  private var callback: Deferred[F, Try[Event]] = _
  private val debugCallNumber                   = new AtomicInteger()
  private val debugMode                         = false

  private def waitForRequest: Receive = { case req: Request[F] =>
    eval {
      callback = req.callback
    } ++ req.event ~> req.receiver ++ switch(waitForResponse)
  }

  private def waitForResponse: Receive = {
    case Start => unit
    case Stop  =>
      flow {
        if callback != null then
          suspend(
            callback.complete(scala.util.Failure(ChannelInterruptedException("channel has been closed"))).map(_ => ())
          )
        else unit
      }
    case req: Request[F] =>
      suspend(
        req.callback
          .complete(scala.util.Failure(IllegalChannelStateException("the current request is not completed yet")))
          .map(_ => ())
      )
    case Failure(_, error) =>
      suspend(callback.complete(scala.util.Failure(error)).map(_ => ())) ++ resetAndWaitForRequest
    case event =>
      suspend(callback.complete(scala.util.Success(event)).map(_ => ())) ++ debug(
        s"resetAndWaitForRequest, event: $event"
      ) ++ resetAndWaitForRequest
  }

  private def resetAndWaitForRequest: DslF[F, Unit] =
    eval {
      callback = null
    } ++ switch(waitForRequest)

  def handle: Receive =
    waitForRequest

  /** @deprecated use the version without a callback. */
  @deprecated("use the version without a callback")
  def send[A](event: Event, receiver: ProcessRef, callback: Try[Event] => DslF[F, A]): DslF[F, A] =
    for
      deferred <- suspend(Deferred[F, Try[Event]]())
      _        <- Request(event, deferred, receiver) ~> ref
      result   <- suspend(deferred.get)
      value    <- callback(result)
    yield value

  /** Sends `event` to `receiver` and suspends until a response (or failure) arrives.
    *
    * The implementation acquires the channel's runtime delivery lock for the duration of the call so concurrent senders
    * queue cleanly.
    *
    * @return
    *   `Success(reply)` on a normal response, `Failure(throwable)` on transport error, channel closure, or remote
    *   handler failure.
    */
  def send(event: Event, receiver: ProcessRef): DslF[F, Try[Event]] =
    for
      _        <- lockProcess(ref)
      deferred <- suspend(Deferred[F, Try[Event]]())
      _        <- sendReq(Request(event, deferred, receiver))
      _        <- unlockProcess(ref)
      value    <- suspend(deferred.get)
    yield value

  private def sendReq(req: Request[F]): DslF[F, Unit] =
    eval {
      require(req.callback != null)
      callback = req.callback
    } ++ debug("waitForResponse") ++ switch(waitForResponse) ++ dsl.send(ref, req.event, req.receiver)

  private def debug(message: => String): DslF[F, Unit] =
    if debugMode then
      eval {
        val number = debugCallNumber.incrementAndGet()
        println(s"channel[$ref, $number]: $message")
      }
    else unit

/** Constructors and exceptions for [[Channel]]. */
object Channel:
  /** Base exception type raised by channel operations. */
  sealed class ChannelException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

  /** Raised when the channel is stopped while a request is in flight. */
  final case class ChannelInterruptedException(message: String, cause: Throwable = null)
      extends ChannelException(message, cause)

  /** Raised when a second request arrives before the previous one completes. */
  final case class IllegalChannelStateException(message: String, cause: Throwable = null)
      extends ChannelException(message, cause)

  /** Builds a fresh [[Channel]] with a UUID ref. */
  def apply[F[_]](using Effect[F]): Channel[F] =
    new Channel()

  /** Builds a [[Channel]] pinned to `ref`. */
  def apply[F[_]](ref: ProcessRef)(using Effect[F]): Channel[F] =
    new Channel(ref)

  /** Internal request envelope sent from [[Channel.send]] to the channel's mailbox. */
  final private case class Request[F[_]](event: Event, callback: Deferred[F, Try[Event]], receiver: ProcessRef)
      extends Event
