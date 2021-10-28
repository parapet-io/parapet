package io.parapet.core

import io.parapet.{Envelope, Event}

object Events {
  // Lifecycle events
  sealed trait SystemEvent extends Event

  case object Start extends SystemEvent
  case object Stop extends SystemEvent
  case object Kill extends SystemEvent

  // System events

  case class Failure(envelope: Envelope, error: Throwable) extends Event

  case class DeadLetter(envelope: Envelope, error: Throwable) extends Event

  object DeadLetter {
    def apply(f: Failure): DeadLetter = new DeadLetter(f.envelope, f.error)
  }
}
