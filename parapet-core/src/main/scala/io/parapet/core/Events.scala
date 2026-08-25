package io.parapet.core

import io.parapet.runtime.Envelope
import io.parapet.{Event, ProcessRef}

/** System-defined [[Event]]s emitted by the parapet runtime.
  *
  * Application code observes these by adding clauses to a process's `handle`. They cover the lifecycle (start / stop /
  * kill) and the failure model (handler errors and undeliverable envelopes).
  */
object Events {

  /** Marker trait for runtime-issued lifecycle events. Not generally extended by user code. */
  sealed trait SystemEvent extends Event

  /** Prepares a fresh process instance before it receives replayed or live domain events. */
  case object Initialize extends SystemEvent

  /** Signals that a process may begin live operation. During recovery it is delivered after snapshot restoration and
    * journal replay have completed.
    */
  case object Start extends SystemEvent

  /** Delivered after a snapshot has restored a process's state and before journal entries are replayed. [[Start]]
    * follows after replay completes.
    */
  case object Restored extends SystemEvent

  /** Marks the point at which `child` was registered by the receiving parent. Used by recovery to synchronize replay of
    * a dynamic process tree.
    */
  final case class Registered(child: ProcessRef.Unknown) extends SystemEvent

  /** Sent during graceful shutdown. The process should wrap up in-flight work and release resources; child processes
    * are stopped first by the runtime.
    */
  case object Stop extends SystemEvent

  /** Forces immediate termination of a process. Unlike [[Stop]], the runtime does not run the receiver's `handle` for
    * this event - it tears the process down directly.
    */
  case object Kill extends SystemEvent

  /** Carries a handler error back to a process awaiting a correlated reply.
    *
    * Message passing has no call stack, so an error that must reach a waiter travels as an event. Only a delivery that
    * carries [[io.parapet.Scope.Causation]] - one made as a call rather than a notification - is answered this way;
    * every other handler error becomes a [[DeadLetter]].
    *
    * @param envelope
    *   the original envelope whose delivery failed.
    * @param error
    *   the throwable raised by the receiver's handler.
    */
  private[parapet] case class Failure(envelope: Envelope, error: Throwable) extends Event

  /** A delivery that could not be completed - unknown or terminated receiver, a handler that raised, or an event no
    * handler matched - routed to the configured [[io.parapet.core.processes.DeadLetterProcess]].
    *
    * @param sender
    *   the process the undelivered event came from.
    * @param event
    *   the undelivered payload.
    * @param receiver
    *   the process it was addressed to.
    * @param error
    *   why delivery did not complete.
    */
  case class DeadLetter(
      sender: ProcessRef.Unknown,
      event: Event,
      receiver: ProcessRef.Unknown,
      error: Throwable
  ) extends Event

  object DeadLetter {

    private[parapet] def apply(envelope: Envelope, error: Throwable): DeadLetter =
      new DeadLetter(envelope.sender, envelope.event, envelope.receiver, error)

    private[parapet] def apply(f: Failure): DeadLetter = apply(f.envelope, f.error)
  }
}
