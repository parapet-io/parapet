package io.parapet.core

import cats.effect.Concurrent
import cats.effect.ExitCase
import cats.effect.concurrent.Deferred
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events.{Failure, Stop}
import io.parapet.core.api.Event

import scala.util.Try

/** Channel is a process that implements strictly synchronous request-reply dialog.
  * The channel sends an event to a receiver and then waits for a response in one step, i.e. it blocks asynchronously
  * until it receives a response. Doing any other sequence, e.g., sending two request or reply events in a row
  * will result in a failure returned to the sender.
  *
  * @tparam F an effect type
  */
class Channel[F[_]: Concurrent](clientRef:ProcessRef = null) extends Process[F] {

  import io.parapet.core.Channel._

  import dsl._

  private val ct: Concurrent[F] = implicitly[Concurrent[F]]

  private var callback: Deferred[F, Try[Event]] = _

  private def waitForRequest: Receive = { case req: Request[F] =>
    eval {
      callback = req.cb
    } ++ req.e ~> req.receiver ++ switch(waitForResponse)
  }

  private def waitForResponse: Receive = {
    case Stop =>
      flow {
        if (callback != null) {
          suspend(callback.complete(scala.util.Failure(new InterruptedException("channel has been closed"))))
        } else {
          unit
        }
      }
    case req: Request[F] =>
      suspend(req.cb.complete(scala.util.Failure(new IllegalStateException("the current request is not completed yet"))))
    case Failure(_, err) =>
      suspend(callback.complete(scala.util.Failure(err))) ++ resetAndWaitForRequest
    case e =>
      suspend(callback.complete(scala.util.Success(e))) ++ resetAndWaitForRequest
  }

  private def resetAndWaitForRequest: DslF[F, Unit] = {
    eval {
      callback = null
    } ++ switch(waitForRequest)
  }

  def handle: Receive = waitForRequest

  /** Sends an event to the receiver and blocks until it receives a response.
    *
    * @param event    the event to send
    * @param receiver the receiver
    * @param cb       a callback function that gets executed once a response received
    * @return Unit
    */
  def send(event: Event, receiver: ProcessRef, cb: Try[Event] => DslF[F, Unit]): DslF[F, Unit] =
    for {
      d <- suspend(Deferred[F, Try[Event]])
      _ <- Request(event, d, receiver) ~> ref
      res <-
        suspend(
          ct.guaranteeCase(d.get){
          case ExitCase.Canceled =>
            (resetAndWaitForRequest ++ cb(scala.util.Failure(ChannelInterruptedException)))
              .foldMap[F](DslInterpreter.instance.interpret(clientRef, clientRef))
          case ExitCase.Error(err) =>
            (resetAndWaitForRequest ++ cb(scala.util.Failure(new ChannelException(err))))
              .foldMap[F](DslInterpreter.instance.interpret(clientRef, clientRef))
          case ExitCase.Completed => ct.delay(println("completed"))})
      _ <- cb(res)
    } yield ()

}

object Channel {

  sealed class ChannelException(cause:Throwable) extends RuntimeException(cause)
  case object ChannelInterruptedException extends ChannelException(null)

  def apply[F[_]: Concurrent]: Channel[F] = new Channel()

  private case class Request[F[_]](e: Event, cb: Deferred[F, Try[Event]], receiver: ProcessRef) extends Event

}
