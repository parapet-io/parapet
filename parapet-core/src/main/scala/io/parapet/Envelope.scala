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
  */
final case class Envelope(
    sender: ProcessRef.Unknown,
    event: Event,
    receiver: ProcessRef.Unknown,
    scope: Scope = Scope.empty
):
  self =>

  /** Reserved for future tracing/debugging support; currently always `0`. */
  val ts: Long = 0L

  /** Reserved for future tracing/debugging support; currently always empty. */
  val id: String = ""

  /** Returns a copy of this envelope with [[event]] replaced by `value`. The sender, receiver, and scope are preserved -
    * handy for [[io.parapet.core.EventTransformer]] stages.
    */
  def event(value: Event): Envelope =
    self.copy(event = value)

  override def toString: String =
    if scope.isEmpty then s"Envelope(id:$id, sender:$sender, event:$event, receiver:$receiver, ts:$ts)"
    else s"Envelope(id:$id, sender:$sender, event:$event, receiver:$receiver, ts:$ts, scope:${scope.entries.toMap})"
