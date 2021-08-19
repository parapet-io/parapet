package io.parapet.core

import io.parapet.core.api.Event

object Events {
  // Lifecycle events
  case object Start extends Event

  case object Stop extends Event

  case object Kill extends Event

  // System events

  case class Failure(envelope: Envelope, error: Throwable) extends Event

  case class DeadLetter(envelope: Envelope, error: Throwable) extends Event

  object DeadLetter {
    def apply(f: Failure): DeadLetter = new DeadLetter(f.envelope, f.error)
  }
}
