package io.parapet.core

import io.parapet.core.Dsl.DslF
import io.parapet.core.Events.{Failure, Start, Stop}
import io.parapet.effect.{Deferred, Effect}
import io.parapet.effect.Monad.*
import io.parapet.{Event, ProcessRef}

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.util.Try

/** A request/response helper process for synchronous-style interaction inside the asynchronous parapet runtime.
  *
  * A `Channel` is a small state machine: it accepts one [[Channel.Request]], forwards the embedded event to the target
  * process, and completes the request's [[Deferred]] with the next response delivered back by that target. While a
  * request is in flight the channel rejects additional requests; this enforces a strict "one outstanding call"
  * discipline.
  *
  * Callers typically construct a channel, register it under their own process, and use [[send]] to obtain a `Try[Out]`
  * representing the eventual reply.
  *
  * `Channel` is intentionally not a reliable-delivery or correlated-RPC abstraction. It waits for the next response
  * from the active receiver; if a timed-out receiver later replies while another request to the same receiver is in
  * flight, the application protocol must distinguish that stale reply. TODO
  *
  * @tparam In
  *   events this channel is allowed to send.
  * @tparam Out
  *   expected response event type.
  *
  * @param ref
  *   optional fixed reference; defaults to a fresh UUID.
  */
class Channel[F[_], In <: Event, Out <: Event](
    override val ref: ProcessRef[Event] = ProcessRef.jdkUUIDRef[Event]
)(using Effect[F], ClassTag[Out])
    extends Process[F, Event, Event]:
  import Channel.*
  import dsl.*

  private val runtimeDsl = summon[Dsl.RuntimeOps.Aux[F]]
  import runtimeDsl.*

  private var inFlight: Option[InFlight[F, Out]] = None
  private val debugCallNumber                    = new AtomicInteger()
  private val requestIds                         = new AtomicLong()
  private val debugMode                          = false

  private def waitForRequest: Receive = {
    case req: Request[F, Out] @unchecked => sendReq(req)
    case Timeout(_)                      => unit
  }

  private def waitForResponse: Receive = {
    case Start => unit
    case Stop  =>
      flow {
        inFlight match
          case Some(active) =>
            complete(active, scala.util.Failure(ChannelInterruptedException("channel has been closed")))
          case None => unit
      }
    case req: Request[F, Out] @unchecked =>
      suspend(
        req.result
          .complete(scala.util.Failure(IllegalChannelStateException("the current request is not completed yet")))
          .map(_ => ())
      )
    case Timeout(id) =>
      inFlight match
        case Some(active) if active.id == id =>
          active.timeout.fold(unit)(timeout =>
            completeAndReset(active, scala.util.Failure(ChannelTimeoutException(timeout)))
          )
        case _ => unit
    case Failure(_, error) =>
      inFlight match
        case Some(active) => completeAndReset(active, scala.util.Failure(error))
        case None         => unit
    case event =>
      withSender { sender =>
        inFlight match
          case Some(active) if active.receiver == sender =>
            debug(
              s"resetAndWaitForRequest, event: $event"
            ) ++ completeAndReset(active, castResponse(event))
          case Some(active) =>
            completeAndReset(
              active,
              scala.util.Failure(
                UnexpectedChannelResponseException(
                  s"expected response from ${active.receiver}, but received $event from $sender"
                )
              )
            )
          case None => unit
      }
  }

  private def resetAndWaitForRequest: DslF[F, Unit] =
    eval {
      inFlight = None
    } ++ switch(waitForRequest)

  def handle: Receive = waitForRequest

  /** Sends `event` to `receiver` and suspends until a response (or failure) arrives.
    *
    * The implementation acquires the channel's runtime delivery lock while installing the request. If another request
    * is already in flight, this call completes with [[IllegalChannelStateException]].
    *
    * @return
    *   `Success(reply)` on a normal response, `Failure(error)` on error.
    */
  def send[E <: In](event: E, receiver: ProcessRef[? >: E]): DslF[F, Try[Out]] =
    send(event, receiver, None)

  /** Sends `event` to `receiver` and waits up to `timeout` for a response.
    *
    * If the timeout elapses before a response or failure arrives, the call completes with [[ChannelTimeoutException]]
    * and the channel is reset for the next request.
    */
  def send[E <: In](event: E, receiver: ProcessRef[? >: E], timeout: FiniteDuration): DslF[F, Try[Out]] =
    send(event, receiver, Some(timeout))

  private def send[E <: In](
      event: E,
      receiver: ProcessRef[? >: E],
      timeout: Option[FiniteDuration]
  ): DslF[F, Try[Out]] =
    for
      requestId <- eval(requestIds.incrementAndGet())
      deferred  <- suspend(Deferred[F, Try[Out]]())
      request = Request(requestId, event, deferred, receiver, timeout)
      _     <- lockProcess(ref)
      _     <- sendReq(request).guarantee(unlockProcess(ref))
      value <- suspend(deferred.get)
    yield value

  private def sendReq(req: Request[F, Out]): DslF[F, Unit] =
    eval {
      inFlight match
        case Some(_) => false
        case None    =>
          inFlight = Some(InFlight(req.id, req.result, req.receiver, req.timeout))
          true
    }.flatMap {
      case true =>
        req.timeout.fold(unit)(timeout => fork(delay(timeout) ++ Timeout(req.id) ~> ref).void) ++
          debug("waitForResponse") ++ switch(waitForResponse) ++
          dsl.send(ref, req.event, req.receiver.asInstanceOf[ProcessRef[Event]])
      case false =>
        suspend(
          req.result
            .complete(scala.util.Failure(IllegalChannelStateException("the current request is not completed yet")))
            .map(_ => ())
        )
    }

  private def castResponse(event: Event): Try[Out] =
    event match
      case out: Out => scala.util.Success(out)
      case other    =>
        scala.util.Failure(
          UnexpectedChannelResponseException(
            s"expected response matching ${summon[ClassTag[Out]]}, but received ${other.getClass.getName}: $other"
          )
        )

  private def complete(active: InFlight[F, Out], result: Try[Out]): DslF[F, Unit] =
    suspend(active.result.complete(result).map(_ => ()))

  private def completeAndReset(active: InFlight[F, Out], result: Try[Out]): DslF[F, Unit] =
    resetAndWaitForRequest ++ complete(active, result)

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

  /** Raised when a request waits longer than the configured timeout. */
  final case class ChannelTimeoutException(timeout: FiniteDuration, cause: Throwable = null)
      extends ChannelException(s"channel request timed out after $timeout", cause)

  /** Raised when the channel receives an event that cannot satisfy the expected reply contract. */
  final case class UnexpectedChannelResponseException(message: String, cause: Throwable = null)
      extends ChannelException(message, cause)

  /** Builds a fresh [[Channel]] with a UUID ref. */
  def apply[F[_]](using Effect[F]): Channel[F, Event, Event] =
    new Channel[F, Event, Event]()

  /** Builds a fresh typed [[Channel]] with a UUID ref. */
  def apply[F[_], In <: Event, Out <: Event](using Effect[F], ClassTag[Out]): Channel[F, In, Out] =
    new Channel[F, In, Out]()

  /** Builds a [[Channel]] pinned to `ref`. */
  def apply[F[_]](ref: ProcessRef[Event])(using Effect[F]): Channel[F, Event, Event] =
    new Channel[F, Event, Event](ref)

  /** Builds a typed [[Channel]] pinned to `ref`. */
  def apply[F[_], In <: Event, Out <: Event](ref: ProcessRef[Event])(using
      Effect[F],
      ClassTag[Out]
  ): Channel[F, In, Out] =
    new Channel[F, In, Out](ref)

  /** Internal request envelope sent from [[Channel.send]] to the channel's mailbox. */
  final private case class Request[F[_], Out <: Event](
      id: Long,
      event: Event,
      result: Deferred[F, Try[Out]],
      receiver: ProcessRef.Unknown,
      timeout: Option[FiniteDuration]
  ) extends Event

  final private case class InFlight[F[_], Out <: Event](
      id: Long,
      result: Deferred[F, Try[Out]],
      receiver: ProcessRef.Unknown,
      timeout: Option[FiniteDuration]
  )

  final private case class Timeout(id: Long) extends Event
