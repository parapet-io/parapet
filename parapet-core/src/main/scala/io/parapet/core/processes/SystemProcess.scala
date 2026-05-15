package io.parapet.core.processes

import io.parapet.{Event, ProcessRef}
import io.parapet.ProcessRef.{DeadLetterRef, SystemRef}
import io.parapet.core.Events.{DeadLetter, Failure}
import io.parapet.core.Process

/** Built-in singleton process pinned to [[io.parapet.ProcessRef.SystemRef]] that handles runtime-level failures.
  *
  * The runtime sends a [[Failure]] here whenever a normal sender cannot intercept its own routing failure (for example,
  * when the failed envelope's sender ref is itself unknown). The system process re-wraps the failure as a
  * [[DeadLetter]] and routes it to the [[DeadLetterProcess]].
  */
class SystemProcess[F[_]] extends Process[F, Event, Event] {

  override val name: String           = SystemRef.value
  override val ref: ProcessRef[Event] = SystemRef
  override val handle: Receive        = { case f: Failure =>
    dsl.send(DeadLetter(f), DeadLetterRef)
  }
}
