package io.parapet.examples.process

import cats.effect.IO
import io.parapet.CatsApp
import io.parapet.core.Event.Start
import io.parapet.core.{Process, ProcessRef}

object ProcessCreation extends CatsApp {

  // Stateful process
  class GenericProcess[F[_]] extends Process[F] {

    import dsl._

    private var counter = 0

    // Optionally you can override `ref` and `name`.
    // NOTE: `ref` must be unique
    // override val ref: ProcessRef = ProcessRef("your_value")
    // override val name: String = "your_value"

    // valid values: -1; [1, Int.MaxValue)
    // -1 means unbounded; -1 is a default value unless it's set globally
    // override val bufferSize: Int = 1000

    override def handle: Receive = {
      case Start => eval {
        counter = counter + 1
        println("counter=" + counter)
      }
    }
  }

  // IO process
  class IOProcess extends Process[IO] {

    import dsl._

    override def handle: Receive = {
      case Start => unit // TODO
    }
  }

  val anonymousProcess: Process[IO] = Process[IO](ref => {
    case Start => dsl.eval(println("ref=" + ref))
  })

  val usingBuilder: Process[IO] = Process.builder[IO](ref => {
    case Start => dsl.eval(println("ref=" + ref))
  }).ref(ProcessRef("usingBuilder")) // sets a process reference
    .name("usingBuilder") // sets a process name
    .bufferSize(1000) // set a process queue size limit
    .build

  override def processes: IO[Seq[Process[IO]]] = {
    IO(Seq(
      new GenericProcess[IO](),
      anonymousProcess,
      usingBuilder
    ))
  }
}
