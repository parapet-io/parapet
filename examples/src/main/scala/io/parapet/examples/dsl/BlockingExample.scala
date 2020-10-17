package io.parapet.examples.dsl

import cats.effect.IO
import io.parapet.CatsApp
import io.parapet.core.Event.Start
import io.parapet.core.Process

object BlockingExample extends CatsApp {

  import dsl._

  class BlockingProcess extends Process[IO] {
    override def handle: Receive = {
      case Start => blocking(eval(while (true) {})) ++ eval(println("now"))
    }
  }


  override def processes: IO[Seq[Process[IO]]] = IO(Seq(new BlockingProcess))
}
