package io.parapet.core.processes

import io.parapet.{Event, Process, ProcessRef}

/** Built-in process that silently swallows every event sent to it.
  *
  * Registered automatically by the runtime under [[io.parapet.ProcessRef.NoopRef]] so user code can route messages to a
  * known sink (e.g., during shutdown) without producing dead-letter noise.
  */
class Noop[F[_]] extends Process[F, Event, Nothing] {
  override val ref: ProcessRef[Event] = ProcessRef.NoopRef

  override def handle: Receive = { case _ =>
    dsl.unit
  }
}
