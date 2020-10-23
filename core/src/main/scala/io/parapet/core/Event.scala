package io.parapet.core

trait Event

object Event {

  // Lifecycle events
  case object Start extends Event

  case object Stop extends Event

  case object Kill extends Event

  // System events
  case class Envelope(sender: ProcessRef, event: Event, receiver: ProcessRef) extends Event {
    // timestamp in nanos for debugging purposes
    val ts: Long = System.nanoTime()

    override def toString: String = s"Envelope(sender=$sender, event=$event, receiver=$receiver, ts=$ts)"
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