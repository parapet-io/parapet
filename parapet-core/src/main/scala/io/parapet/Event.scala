package io.parapet

/** Marker trait for any value that can flow between [[Process]] instances.
  *
  * Every message exchanged through the parapet runtime - system signals, user commands, dead-letter notifications,
  * network frames - must extend `Event`. Events are typically implemented as immutable case classes so that they can be
  * safely shared between fibers without defensive copying.
  *
  * Two general-purpose payload events are provided in the companion object. Domain code is expected to define its own
  * event hierarchies (often as a sealed family) for type-safe pattern matching inside a process's [[Process.handle]].
  */
trait Event

/** The events parapet itself defines: those the runtime emits ([[Event.SystemEvent]]) and general-purpose payload
  * carriers for cases where declaring a dedicated type is unnecessary.
  */
object Event:

  /** An event emitted by the runtime rather than by application code.
    *
    * Handlers observe these exactly like domain events. The hierarchy is sealed, so the set is fixed by the runtime.
    */
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

  /** Carries a handler error back to a process awaiting a correlated reply.
    *
    * Message passing has no call stack, so an error that must reach a waiter travels as an event. Only a delivery made
    * as a call - one carrying a causation id - is answered this way; every other handler error becomes a
    * [[DeadLetter]].
    *
    * @param sender
    *   the process whose call failed, and to which this is delivered.
    * @param event
    *   the payload whose delivery failed.
    * @param receiver
    *   the process whose handler raised.
    * @param error
    *   the throwable raised by the receiver's handler.
    */
  final private[parapet] case class Failure(
      sender: ProcessRef.Unknown,
      event: Event,
      receiver: ProcessRef.Unknown,
      error: Throwable
  ) extends SystemEvent

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
  final case class DeadLetter(
      sender: ProcessRef.Unknown,
      event: Event,
      receiver: ProcessRef.Unknown,
      error: Throwable
  ) extends SystemEvent

  object DeadLetter:
    /** Lifts a [[Failure]] no waiter consumed into a dead letter, preserving the original routing. */
    private[parapet] def apply(f: Failure): DeadLetter =
      new DeadLetter(f.sender, f.event, f.receiver, f.error)

  /** An [[Event]] carrying a raw byte payload.
    *
    * Useful for transport processes that operate on opaque buffers and delegate decoding to upstream handlers.
    *
    * @param data
    *   the raw bytes; ownership is transferred to the event and must not be mutated after construction.
    */
  final case class ByteEvent(data: Array[Byte]) extends Event:
    override def toString: String = new String(data)

  /** An [[Event]] carrying a single string payload.
    *
    * @param value
    *   the payload text.
    */
  final case class StringEvent(value: String) extends Event:
    override def toString: String = value
