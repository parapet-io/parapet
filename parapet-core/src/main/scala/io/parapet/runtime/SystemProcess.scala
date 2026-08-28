package io.parapet.runtime

import io.parapet.Event.{DeadLetter, Failure}
import io.parapet.ProcessRef.{DeadLetterRef, SystemRef}
import io.parapet.{Event, Process, ProcessRef}

/** The runtime's own process, pinned to [[io.parapet.ProcessRef.SystemRef]]. It is the sender of events the runtime
  * originates.
  */
class SystemProcess[F[_]] extends Process[F, Event] {

  override val name: String           = SystemRef.value
  override val ref: ProcessRef[Event] = SystemRef
  override val handle: Receive        = { case f: Failure =>
    dsl.send(DeadLetter(f), DeadLetterRef)
  }
}
