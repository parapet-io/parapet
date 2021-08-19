package io.parapet.core

import io.parapet.core.api.Event

case class Envelope(sender: ProcessRef, event: Event, receiver: ProcessRef) { self =>
  // timestamp in nanos for debugging purposes
  val ts: Long = 0L //System.nanoTime() // todo debug
  val id: String = "" //UUID.randomUUID().toString // todo debug

  def event(value: Event): Envelope = self.copy(event = value)

  override def toString: String = s"Envelope(id:$id, sender:$sender, event:$event, receiver:$receiver, ts:$ts)"
}
