package io.parapet.core

import io.parapet.ProcessRef

/** Exceptions raised by the parapet runtime when routing or process lifecycle invariants are violated.
  *
  * These are surfaced through [[io.parapet.core.Events.Failure]] and (when unhandled) end up at the
  * [[io.parapet.core.processes.DeadLetterProcess]].
  */
package object exceptions {

  /** Raised when an envelope targets a [[ProcessRef]] that is not registered. */
  case class UnknownProcessException(message: String) extends RuntimeException(message)

  object UnknownProcessException {

    /** Convenience constructor producing a standard message for `ref`. */
    def apply(ref: ProcessRef.Unknown): UnknownProcessException =
      new UnknownProcessException(s"process: '$ref' doesn't exist")
  }

  /** Wraps a throwable raised inside a process's `handle` so it can be propagated as a
    * [[io.parapet.core.Events.Failure]].
    */
  case class EventHandlingException(message: String = "", cause: Throwable = null)
      extends RuntimeException(message, cause)

  /** Raised when an envelope cannot be delivered for reasons other than an unknown receiver (e.g. transport failure).
    */
  case class EventDeliveryException(message: String = "", cause: Throwable = null)
      extends RuntimeException(message, cause)

  /** Raised when a process's mailbox is full and the runtime cannot enqueue. */
  case class EventQueueIsFullException(message: String) extends RuntimeException(message)

  /** Raised when an event arrives at a process whose `handle` is not defined for it. */
  case class EventMatchException(message: String) extends RuntimeException(message)

  /** Raised when a process is used before its [[io.parapet.core.Process.init]] hook ran. */
  case class UninitializedProcessException(message: String) extends RuntimeException(message)

  /** Raised when a stopped process receives further events. */
  case class ProcessStoppedException(message: String) extends RuntimeException(message)

  object ProcessStoppedException {

    /** Convenience constructor producing a standard message for `ref`. */
    def apply(ref: ProcessRef.Unknown): ProcessStoppedException =
      new ProcessStoppedException(s"process: '$ref' is already stopped")
  }

}
