package io.parapet.core

import cats.effect.Concurrent
import cats.effect.concurrent.Deferred
import io.parapet.core.Channel.Request
import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.{Failure, Start, Stop}
import io.parapet.implicits._
import io.parapet.syntax.EventSyntax

import scala.util.{Success, Try, Failure => SFailure}

class Channel[F[_] : Concurrent] extends Process[F] with EventSyntax[F]{

  import dsl._

  private var callback: Deferred[F, Try[Event]] = _

  private def waitForRequest: Receive = {
    case Start => empty
    case Stop => empty
    case req: Request[F] =>
      eval {
        callback = req.cb
      } ++ req.e ~> req.receiver ++ switch(waitForResponse)
  }

  private def waitForResponse: Receive = {
    case Stop => suspend(callback.complete(SFailure(new InterruptedException("channel has been closed"))))
    case req: Request[F] =>
      suspend(req.cb.complete(SFailure(new IllegalStateException("current request is not completed yet"))))
    case Failure(_, err) => suspend(callback.complete(SFailure(err))) // receiver has failed to process request
    case e =>
      suspend(callback.complete(Success(e))) ++
        eval(callback = null) ++
        switch(waitForRequest)
  }

  def handle: Receive = waitForRequest

  def send(e: Event, receiver: ProcessRef, cb: Try[Event] => DslF[F, Unit]): DslF[F, Unit] = {
    suspendWith(Deferred[F, Try[Event]]) { d =>
      Request(e, d, receiver) ~> selfRef ++ suspendWith(d.get)(cb)
    }
  }

}

object Channel {

  private case class Request[F[_]](e: Event, cb: Deferred[F, Try[Event]], receiver: ProcessRef) extends Event

}