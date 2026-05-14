package io.parapet.core.processes

/** A [[io.parapet.core.Process]] base class that bundles an explicit per-instance state value `S`.
  *
  * Mostly a documentation aid: it doesn't add behavior beyond the [[state]] field but encourages a clean separation
  * between the process's identity and its mutable state.
  *
  * @param state
  *   initial state; the subclass owns its mutation discipline.
  */
abstract class ProcessWithState[F[_], S](state: S) extends io.parapet.core.Process[F, io.parapet.Event]
