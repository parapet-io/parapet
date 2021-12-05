package io.parapet.core.processes

import io.parapet.core.Process

class Noop[F[_]] extends Process[F] {
  override def handle: Receive = { case _ =>
    dsl.unit
  }
}
