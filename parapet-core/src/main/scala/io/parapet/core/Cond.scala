package io.parapet.core

import io.parapet.core.Cond.*
import io.parapet.core.Dsl.DslF
import io.parapet.effect.Effect
import io.parapet.{Event, ProcessRef}

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

/** A one-shot conditional waiter implemented as a [[Process]].
  *
  * The originating sender issues a [[Cond.Start]] event; the `Cond` then watches all subsequent events and replies once
  * with a [[Cond.Result]] containing the first event for which `predicate` returned `true`. If no matching event
  * arrives within `timeout` the result is `None`.
  *
  * Useful for "wait until X happens" patterns without manually wiring callbacks.
  *
  * @param predicate
  *   decides whether an incoming event satisfies the condition.
  * @param timeout
  *   maximum time to wait for a satisfying event before responding with `Result(None)`.
  */
class Cond[F[_]](predicate: Event => Boolean, timeout: FiniteDuration)(using Effect[F]) extends Process[F]:
  import dsl.*

  private val done                = new AtomicBoolean()
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
      _       <- if success then result ~> replyTo else unit
    yield ()

/** Events used by [[Cond]]. */
object Cond:
  /** Begins watching events. The sender of this event is who [[Result]] will be sent to. */
  case object Start extends Event

  /** Reply emitted exactly once when the condition fires or the timeout elapses.
    *
    * @param event
    *   `Some(event)` if a matching event arrived, `None` on timeout.
    */
  final case class Result(event: Option[Event]) extends Event
