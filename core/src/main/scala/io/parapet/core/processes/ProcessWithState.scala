package io.parapet.core.processes

abstract class ProcessWithState[F[_], S](state: S) extends io.parapet.core.Process[F]
