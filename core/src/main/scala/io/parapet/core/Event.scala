package io.parapet.core

import java.util.UUID

trait Event {
  final val id: String = UUID.randomUUID().toString
}

object Event {

  // Lifecycle events
  case object Start extends Event

  case object Stop extends Event

  // System events
  case class Envelope(sender: ProcessRef, event: Event, receiver: ProcessRef) extends Event

  case class Failure(envelope: Envelope, error: Throwable) extends Event

  case class DeadLetter(envelope: Envelope, error: Throwable) extends Event

  object DeadLetter {
    def apply(f: Failure): DeadLetter = new DeadLetter(f.envelope, f.error)
  }

}