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

  case class Failure(pRef: ProcessRef, event: Event, error: Throwable) extends Event

  case class DeadLetter(failure: Failure) extends Event

}