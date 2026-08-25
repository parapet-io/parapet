package io.parapet.runtime

import io.parapet.runtime.Envelope
import io.parapet.{Event, ProcessRef}

/** A delivery wrapper that pairs an [[Event]] with its routing metadata.
  *
  * @param sender
  *   the originating process; may be [[ProcessRef.UndefinedRef]] for events produced by the runtime itself.
  * @param event
  *   the payload being delivered.
  * @param receiver
  *   the addressed process.
  * @param scope
  *   per-delivery metadata; defaults to [[Scope.empty]].
  * @param id
  *   unique identity; defaults to a fresh [[Envelope.nextId]].
  */
final case class Envelope(
    sender: ProcessRef.Unknown,
    event: Event,
    receiver: ProcessRef.Unknown,
    scope: Scope = Scope.empty,
    id: Long = Envelope.nextId()
):
  self =>

  /** Reserved for future tracing/debugging support; currently always `0`. */
  val ts: Long = 0L

  /** Id of the envelope that caused this one. */
  def cause: Long = scope.get(Scope.Cause).getOrElse(0L)

  /** A scope in which this envelope is the cause: events emitted under it are recorded as caused by this envelope. */
  def causalScope: Scope = scope.put(Scope.Cause, id)

  /** Returns a copy of this envelope with [[event]] replaced by `value`. */
  def event(value: Event): Envelope =
    self.copy(event = value)

  /** This delivery, described as undeliverable. */
  private[parapet] def deadLetter(error: Throwable): Event.DeadLetter =
    Event.DeadLetter(sender, event, receiver, error)

  /** This delivery, described as failed, for return to a waiting caller. */
  private[parapet] def failure(error: Throwable): Event.Failure =
    Event.Failure(sender, event, receiver, error)

  override def toString: String =
    if scope.isEmpty then s"Envelope(id:$id, cause:$cause, sender:$sender, event:$event, receiver:$receiver)"
    else
      s"Envelope(id:$id, cause:$cause, sender:$sender, event:$event, receiver:$receiver, scope:${scope.entries.toMap})"

object Envelope:
  private val idCounter = new java.util.concurrent.atomic.AtomicLong(0L)

  /** Monotonically increasing envelope id (starts at 1; `0L` denotes "none"/root). */
  def nextId(): Long = idCounter.incrementAndGet()

  /** Advances the counter so ids minted from now on exceed `id`. */
  def continueIdAfter(id: Long): Unit =
    idCounter.updateAndGet(current => math.max(current, id))
    ()
