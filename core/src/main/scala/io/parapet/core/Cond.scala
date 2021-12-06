package io.parapet.core

import cats.effect.Concurrent
import io.parapet.core.Cond._
import io.parapet.core.Dsl.DslF
import io.parapet.{Event, ProcessRef}

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

class Cond[F[_] : Concurrent](reply: ProcessRef, cond: Event => Boolean, timeout: FiniteDuration)
  extends io.parapet.core.Process[F] {

  import dsl._

  private val done = new AtomicBoolean()

  override def handle: Receive = {
    case Cond.Start => fork(delay(timeout) ++ respond(Result(Option.empty)))
    case event => eval(Try(cond(event))).flatMap {
      case scala.util.Success(true) => respond(Result(Option(event)))
      case scala.util.Success(false) => unit
      case scala.util.Failure(err) => raiseError(err)
    }

  }

  private def respond(res: Result): DslF[F, Unit] =
    for {
      success <- eval(done.compareAndSet(false, true))
      _ <- if (success) res ~> reply else unit
    } yield ()
}

object Cond {

  case object Start extends Event

  // better types: Success and Timeout
  case class Result(event: Option[Event]) extends Event

}
