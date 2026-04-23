package io.parapet.core

import io.parapet.core.Dsl.DslF
import io.parapet.core.Events.{Failure, Start, Stop}
import io.parapet.effect.{Deferred, Effect}
import io.parapet.effect.Monad.*
import io.parapet.{Event, ProcessRef}

import java.util.concurrent.atomic.AtomicInteger
import scala.util.Try

class Channel[F[_]](override val ref: ProcessRef = ProcessRef.jdkUUIDRef)(using Effect[F]) extends Process[F]:
  import Channel.*
  import dsl.*

  private var callback: Deferred[F, Try[Event]] = _
  private val debugCallNumber = new AtomicInteger()
  private val debugMode = false

  private def waitForRequest: Receive = {
    case req: Request[F] =>
      eval {
        callback = req.callback
      } ++ req.event ~> req.receiver ++ switch(waitForResponse)
  }

  private def waitForResponse: Receive = {
    case Start => unit
    case Stop =>
      flow {
        if callback != null then
          suspend(callback.complete(scala.util.Failure(ChannelInterruptedException("channel has been closed"))).map(_ => ()))
        else unit
      }
    case req: Request[F] =>
      suspend(
        req.callback.complete(scala.util.Failure(IllegalChannelStateException("the current request is not completed yet")))
          .map(_ => ())
      )
    case Failure(_, error) =>
      suspend(callback.complete(scala.util.Failure(error)).map(_ => ())) ++ resetAndWaitForRequest
    case event =>
      suspend(callback.complete(scala.util.Success(event)).map(_ => ())) ++ debug(s"resetAndWaitForRequest, event: $event") ++ resetAndWaitForRequest
  }

  private def resetAndWaitForRequest: DslF[F, Unit] =
    eval {
      callback = null
    } ++ switch(waitForRequest)

  def handle: Receive =
    waitForRequest

  @deprecated("use the version without a callback")
  def send[A](event: Event, receiver: ProcessRef, callback: Try[Event] => DslF[F, A]): DslF[F, A] =
    for
      deferred <- suspend(Deferred[F, Try[Event]]())
      _ <- Request(event, deferred, receiver) ~> ref
      result <- suspend(deferred.get)
      value <- callback(result)
    yield value

  def send(event: Event, receiver: ProcessRef): DslF[F, Try[Event]] =
    for
      _ <- lock(ref)
      deferred <- suspend(Deferred[F, Try[Event]]())
      _ <- sendReq(Request(event, deferred, receiver))
      _ <- unlock(ref)
      value <- suspend(deferred.get)
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

object Channel:
  sealed class ChannelException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)
  final case class ChannelInterruptedException(message: String, cause: Throwable = null)
      extends ChannelException(message, cause)
  final case class IllegalChannelStateException(message: String, cause: Throwable = null)
      extends ChannelException(message, cause)

  def apply[F[_]](using Effect[F]): Channel[F] =
    new Channel()

  def apply[F[_]](ref: ProcessRef)(using Effect[F]): Channel[F] =
    new Channel(ref)

  private final case class Request[F[_]](event: Event, callback: Deferred[F, Try[Event]], receiver: ProcessRef)
      extends Event
