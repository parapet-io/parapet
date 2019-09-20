package io.parapet.examples.peer

import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.Start
import io.parapet.core.{Process, Stream}

class Consumer[F[_]](stream: Stream[F]) extends Process[F] {

  import dsl._

  private def rcv: DslF[F, Unit] = {
    def step: DslF[F, Unit] = flow {
      blocking {
        suspendWith(stream.read)(data => {
          eval(println(new String(data)))
        })
      } ++ step
    }
    step
  }

  override def handle: Receive = {
    case Start => rcv
  }
}
