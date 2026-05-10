package io.parapet.core

import io.parapet.{Envelope, Event}

/** System-defined [[Event]]s emitted by the parapet runtime.
  *
  * Application code observes these by adding clauses to a process's `handle`. They cover the lifecycle (start / stop /
  * kill) and the failure model (handler errors and undeliverable envelopes).
  */
object Events {

  /** Marker trait for runtime-issued lifecycle events. Not generally extended by user code. */
  sealed trait SystemEvent extends Event

  /** First event delivered to every process after registration. Use to perform initial setup such as registering child
    * processes or sending a "ready" notification.
    */
  case object Start extends SystemEvent

  /** Sent during graceful shutdown. The process should wrap up in-flight work and release resources; child processes
    * are stopped first by the runtime.
    */
  case object Stop extends SystemEvent

  /** Forces immediate termination of a process. Unlike [[Stop]], the runtime does not run the receiver's `handle` for
    * this event - it tears the process down directly.
    */
  case object Kill extends SystemEvent

  /** Notification routed to the original sender when the receiver's `handle` raised. The sender can intercept this to
    * retry, escalate, or log; otherwise the runtime forwards to the dead-letter sink.
    *
    * @param envelope
    *   the original envelope whose delivery failed.
    * @param error
    *   the throwable raised by the receiver's handler.
    */
  case class Failure(envelope: Envelope, error: Throwable) extends Event

  /** Wraps any envelope that could not be delivered (unknown receiver, terminated process, unhandled [[Failure]], etc.)
    * and is routed to the configured [[io.parapet.core.processes.DeadLetterProcess]].
    */
  case class DeadLetter(envelope: Envelope, error: Throwable) extends Event

  object DeadLetter {

    /** Lifts an unhandled [[Failure]] into a [[DeadLetter]] preserving the original envelope and cause.
      */
    def apply(f: Failure): DeadLetter = new DeadLetter(f.envelope, f.error)
  }
}
