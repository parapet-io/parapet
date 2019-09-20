package io.parapet.examples.peer

import io.parapet.core.{Process, Stream}

class Producer[F[_]](stream: Stream[F]) extends Process[F] {

  import dsl._

  override def handle: Receive = {
    case e => suspend(stream.write(e.toString.getBytes()))
  }
}