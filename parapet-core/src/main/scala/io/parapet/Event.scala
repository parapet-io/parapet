package io.parapet

/** Marker trait for any value that can flow between [[Process]] instances.
  *
  * Every message exchanged through the parapet runtime - system signals, user commands, dead-letter notifications,
  * network frames - must extend `Event`. Events are typically implemented as immutable case classes so that they can be
  * safely shared between fibers without defensive copying.
  *
  * Domain code defines its own event hierarchies (often as a sealed family) for type-safe pattern matching inside a
  * process's [[Process.handle]]. The companion object holds the events the runtime itself emits.
  */
trait Event

/** The events the parapet runtime emits. */
object Event:

  /** An event emitted by the runtime rather than by application code. */
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
  private[parapet] case class Registered(child: ProcessRef.Unknown) extends SystemEvent

  /** Sent during graceful shutdown. The process should wrap up in-flight work and release resources; child processes
    * are stopped first by the runtime.
    */
  case object Stop extends SystemEvent

  /** Forces immediate termination of a process. Unlike [[Stop]], the runtime does not run the receiver's `handle` for
    * this event - it tears the process down directly.
    */
  case object Kill extends SystemEvent

  /** An unexpected error raised while handling an event, reported to its sender.
    *
    * @param sender
    *   the process that sent [[event]].
    * @param event
    *   the event being handled.
    * @param receiver
    *   the process that raised.
    * @param error
    *   the error raised.
    */
  final case class Failure(
      sender: ProcessRef.Unknown,
      event: Event,
      receiver: ProcessRef.Unknown,
      error: Throwable
  ) extends SystemEvent

  /** An event that was not delivered.
    *
    * @param sender
    *   the process that sent [[event]].
    * @param event
    *   the undelivered event.
    * @param receiver
    *   the process it was addressed to.
    * @param error
    *   why it was not delivered.
    */
  final case class DeadLetter(
      sender: ProcessRef.Unknown,
      event: Event,
      receiver: ProcessRef.Unknown,
      error: Throwable
  ) extends SystemEvent

  object DeadLetter:
    private[parapet] def apply(f: Failure): DeadLetter =
      new DeadLetter(f.sender, f.event, f.receiver, f.error)
