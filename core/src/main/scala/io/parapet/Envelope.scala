package io.parapet

final case class Envelope(sender: ProcessRef, event: Event, receiver: ProcessRef):
  self =>

  // Timestamp and id are reserved for future tracing/debugging support.
  val ts: Long = 0L
  val id: String = ""

  def event(value: Event): Envelope =
    self.copy(event = value)

  override def toString: String =
    s"Envelope(id:$id, sender:$sender, event:$event, receiver:$receiver, ts:$ts)"
