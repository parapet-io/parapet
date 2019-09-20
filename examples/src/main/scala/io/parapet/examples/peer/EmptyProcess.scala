package io.parapet.examples.peer

import io.parapet.core.Process

object EmptyProcess {

  def apply[F[_]]: Process[F] = new Process[F] {

    import dsl._

    override def handle: Receive = {
      case _ => unit
    }
  }

}
