package io.parapet.core.processes

import io.parapet.core.Events.{DeadLetter, Failure}
import io.parapet.core.ProcessRef.{DeadLetterRef, SystemRef}
import io.parapet.core.{Process, ProcessRef}

class SystemProcess[F[_]] extends Process[F] {

  override val name: String = SystemRef.value
  override val ref: ProcessRef = SystemRef
  override val handle: Receive = { case f: Failure =>
    dsl.send(DeadLetter(f), DeadLetterRef)
  }
}
