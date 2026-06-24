package io.parapet

/** A delivery wrapper that pairs an [[Event]] with its routing metadata.
  *
  * @param sender
  *   the originating process; may be [[ProcessRef.UndefinedRef]] for events produced by the runtime itself.
  * @param event
  *   the payload being delivered.
  * @param receiver
  *   the addressed process.
  * @param scope
  *   metadata attached to this delivery; defaults to [[Scope.empty]].
  * @param cause
  *   id of the envelope being handled when this one was emitted.
  */
final case class Envelope(
    sender: ProcessRef.Unknown,
    event: Event,
    receiver: ProcessRef.Unknown,
    scope: Scope = Scope.empty,
    cause: Long = 0L
):
  self =>

  /** Reserved for future tracing/debugging support; currently always `0`. */
  val ts: Long = 0L

  /** Unique identity for this envelope.
    */
  val id: Long = Envelope.nextId()

  /** Returns a copy of this envelope with [[event]] replaced by `value`. */
  def event(value: Event): Envelope =
    self.copy(event = value)

  override def toString: String =
    if scope.isEmpty then s"Envelope(id:$id, cause:$cause, sender:$sender, event:$event, receiver:$receiver)"
    else
      s"Envelope(id:$id, cause:$cause, sender:$sender, event:$event, receiver:$receiver, scope:${scope.entries.toMap})"

/** Companion providing cheap, monotonic envelope identity. */
object Envelope:
  private val idCounter = new java.util.concurrent.atomic.AtomicLong(0L)

  /** A cheap, JVM-unique, monotonically increasing envelope id (starts at 1; `0L` denotes "none"/root). */
  def nextId(): Long = idCounter.incrementAndGet()
