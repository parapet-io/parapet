package io.parapet.core

import io.parapet.core.Cond.*
import io.parapet.core.Dsl.DslF
import io.parapet.effect.Effect
import io.parapet.{Event, ProcessRef}

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

class Cond[F[_]](predicate: Event => Boolean, timeout: FiniteDuration)(using Effect[F]) extends Process[F]:
  import dsl.*

  private val done = new AtomicBoolean()
  private var replyTo: ProcessRef = _

  override def handle: Receive = {
    case Start =>
      withSender { sender =>
        eval {
          replyTo = sender
        } ++ fork(delay(timeout) ++ respond(Result(None))).void
      }
    case event =>
      eval(Try(predicate(event))).flatMap {
        case scala.util.Success(true)  => respond(Result(Some(event)))
        case scala.util.Success(false) => unit
        case scala.util.Failure(error) => raiseError(error)
      }
  }

  private def respond(result: Result): DslF[F, Unit] =
    for
      success <- eval(done.compareAndSet(false, true))
      _ <- if success then result ~> replyTo else unit
    yield ()

object Cond:
  case object Start extends Event
  final case class Result(event: Option[Event]) extends Event
