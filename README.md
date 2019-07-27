# Parapet - purely functional library to develop distributed and event driven systems

##  Key features:

* Purely functional library written in scala using Tagless-Final Style and Free Monads thoughtfully designed for people who prefer functional style over imperative
* Rich DSL that allows to write composable and reusable code
* Lightweight and Performat. Library utilizes resources (CPU and Memory) in a smart way, the code is optimized to reduce CPU consumption when your application in idle state
* Built-in support for Cats Effect library
* Extendable. Library can be easily extended to support other Effect System libraries such as Scalaz Task, Monix and etc.

## Getting started

The first thing you need to do is to add *parapet-core* library into your project. You can find  the latest version in maven central:

```scala
libraryDependencies += "io.parapet" %% "core" % "0.0.1-RC1"
```

Once you added the library can start writing your first program, however it's worth to take a few minutes and get familiar with two main approaches to write processes: generic and effect specific, I'll describe both in a minute. For those who aren't familiar with effect systems like Cats Effect I'd strongly recommend you to read some articles about IO monad. Fortunately you don't need to be an expert in Cats Effect in order to use Parapet.

The first aproach we'll consider is Generic. It's recommended to stick to this style when writing processes. Let's develop a simple printer process that will print users requests to the system output.

```scala
import io.parapet.core.{Event, Process}

class Printer[F[_]] extends Process[F] {

  import Printer._ //  import Printer API

  import dsl._ // import DSL operations

  override def handle: Receive = {
    case Print(data) => eval(println(data))
  }
}

object Printer {

  case class Print(data: Any) extends Event

}
```

Now let's write a client that will send `Print` events to the printer process:

```scala
import io.parapet.core.Event.Start
import io.parapet.core.{Process, ProcessRef}
import io.parapet.examples.Printer._ // import Printer API

class PrinterClient[F[_]](printer: ProcessRef) extends Process[F] {
  override def handle: Receive = {
    // Start is a lifecycle event that gets delivered when a process started
    case Start => Print("hello world") ~> printer
  }
}
```

You probably already noticed a new type `ProcessRef`. 

And finally we need to write our application that will run our processes:

```scala
import cats.effect.IO
import io.parapet.CatsApp
import io.parapet.core.Process

object PrinterApp extends CatsApp {
  override def processes: IO[Seq[Process[IO]]] = IO {
    val printer = new Printer[IO]
    val printerClient = new PrinterClient[IO](printer.ref)
    Seq(printer, printerClient)
  }
}
```

This is Cats Effect specific application, meaning it uses  Cats IO type under the hood


