package io.parapet.core

import java.util.UUID

trait Event

object Event {

  // common events

  case class ByteEvent(data: Array[Byte]) extends Event {
    override def toString: String = new String(data)
  }

  case class StringEvent(value:String) extends Event {
    override def toString: String = value
  }

  // Lifecycle events
  case object Start extends Event

  case object Stop extends Event

  case object Kill extends Event

  // System events
  case class Envelope(sender: ProcessRef, event: Event, receiver: ProcessRef) { self =>
    // timestamp in nanos for debugging purposes
    val ts: Long = System.nanoTime()
    val id: String = UUID.randomUUID().toString

    def event(value: Event): Envelope = self.copy(event = value)

    override def toString: String = s"Envelope(id:$id, sender:$sender, event:$event, receiver:$receiver, ts:$ts)"
  }

  case class Failure(envelope: Envelope, error: Throwable) extends Event

  case class DeadLetter(envelope: Envelope, error: Throwable) extends Event

  object DeadLetter {
    def apply(f: Failure): DeadLetter = new DeadLetter(f.envelope, f.error)
  }

  trait Marshall {
    def marshall: Array[Byte]
  }

}
