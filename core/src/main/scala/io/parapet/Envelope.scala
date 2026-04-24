package io.parapet

/** A delivery wrapper that pairs an [[Event]] with its routing metadata.
  *
  * The runtime never moves bare events between processes; it always wraps them in an `Envelope` so that the receiver
  * can identify the sender (e.g., to reply) and the scheduler can dispatch to the correct mailbox.
  *
  * Envelopes are immutable; the [[event]] copy method produces a new envelope when an intermediate stage (such as a
  * transformer) needs to mutate the payload while preserving sender/receiver information.
  *
  * @param sender
  *   the originating process; may be [[ProcessRef.UndefinedRef]] for events produced by the runtime itself.
  * @param event
  *   the payload being delivered.
  * @param receiver
  *   the addressed process; the scheduler routes this envelope to the mailbox identified by this ref.
  */
final case class Envelope(sender: ProcessRef, event: Event, receiver: ProcessRef):
  self =>

  /** Reserved for future tracing/debugging support; currently always `0`. */
  val ts: Long = 0L

  /** Reserved for future tracing/debugging support; currently always empty. */
  val id: String = ""

  /** Returns a copy of this envelope with [[event]] replaced by `value`. The sender and receiver are preserved - handy
    * for [[io.parapet.core.EventTransformer]] stages.
    */
  def event(value: Event): Envelope =
    self.copy(event = value)

  override def toString: String =
    s"Envelope(id:$id, sender:$sender, event:$event, receiver:$receiver, ts:$ts)"
