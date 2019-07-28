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

Let's walk through this code. You start writing your processes by extending `Process` trait and parameterizing it with an effect type. In this example we left so called hole `F[_]` in our `Printer` type which can be any type constructor with a single argument, e.g. `F[_]` is a generic type constructor, cats effect `IO` is a specific type constructor and `IO[Unit]` is a concrete type. Starting from this moment it should become clear what it means for a process to be generic. Simply speaking it means that a process doesn't depend on any specific effect type e.g. `IO`. Thus we can claim that our `Printer` process is surely a generic process. The next step is to define a process API or contract that defines set of events that it can send and recieve. Process contract is an important part of any process specification that should be taken seriously. API defines a protocol that other processes will use in order to communicate with your process. Please remember that it's very important aspect of any process definition and take it seriously. The next step would importing `DSL`, Parapet DSL is a small set of operations that we will consider in details in the next chapters, in this example we need only `eval` operator that  suspends a side effect in `F`, in our Printer process we suspend `println` effectful computation. Finally every process should override `handle` function defined in `Process` trait. `handle` function is a partial function that matches input events and produces an executable `flows`. If you ever tried Akka framework you may find this approach familiar (for the curious, `Receive` is simply a type alias for `PartialFunction[Event, DslF[F, Unit]]`). In our Printer process we match on `Print` event using well known pattern-matching feature in Scala language. If you are new in functional programming I'd strongly recommend to read about pattern-matching, it's a very powerfull feature. 
That's basically it, we consider every important  aspect of our Printer process, let's move forward and write a simple client process that will talk to out Printer.


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

As you already might noticed we are repeating the same steps we made when were writing our `Printer` process: 
* Create a new Process with a  _hole_ `F[_]` in it's type definition
* Extend `io.parapet.core.Process` trait and parametrizing it with generic effect type `F`
* Implement `handle` partial function

Let's consider some new types and operators we have used to write our client: `ProcessRef`, `Start` lifecycle event and `~>` (send) infix operator. Let's start from  `ProcessRef`. `ProcessRef` is a unique process identifier (UUID by default), it represents a process address in Parapet system and *must* be unique, it's recommended to use `ProcessRef` instead of a `Process` object directly unless you are sure you want otherwise. It's not prohibited to use `Process` object directly, however using a process reference may be useful in some scenarios. Let's consider one such case. Imagine we want to dynamically change current `Printer` process in our client so that it will store data in file on disk instead of printing it to the console. We can add a new event `ChangePrinter`:

```scala
case class ChangePrinter(printer: ProcessRef) extends Event
```

Then our client will look like this:

```scala
class PrinterClient[F[_]](private var printer: ProcessRef) extends Process[F] {

  import PrinterClient._
  import dsl._


  override def handle: Receive = {
    case Start => Print("hello world") ~> printer
    case ChangePrinter(newPrinter) => eval(printer = newPrinter)
  }
}

object PrinterClient {

  case class ChangePrinter(printer: ProcessRef) extends Event

}
```

This design cannot be achieved when using direct processes b/c it's not possible to send `Process`  objects,  processes are not serializable in general. One more thing, you can override a `Process#ref` field, only make sure it's unique otherwise Parapet system will return an error during the startup. 

Ok, we almost done! There are few more things left we need to cover: `Start` lifecycle event and `~>` operator. Actually there is nothing special about these two. Parapet has two lifecycle events: 

* `io.parapet.core.Event.Start` is sent to a process once it's created in Parapet system
* `io.parapet.core.Event.Stop` is sent to a process when an application is interrupted with `Ctrl-C` or when some other process sent `Stop` or `Kill` event to that process. The main difference between `Stop` and `Kill` is that in the former case a process can finish processing all pending events before it will receive `Stop` event, whereas `Kill` will interrupt a process and then deliver `Stop` event, all pending events will be discarded. If you familiar with Java `ExecutorService` then you can think of `Stop` as `shutdown` and `Kill` as `shutdownNow`. 

Finally `~>` is most frequently used operator that is defined for any type that extends `io.parapet.core.Event` trait. `~>` is just a symbolic name for `send(event, processRef)` operator.

By this moment we have two processes: `Printer` and `PrinterClient`, nice! But wait. We need to run them somehow, right ?
Fortunately it's extremely easy to do, all we need is to create `PrinterApp` object which represents our application and extend it from `CatsApp` abstract class. `CatsApp` extends ParApp by specifying concrete effect type `IO`:

```scala
abstract class CatsApp extends ParApp[IO]
```

`CatsApp` is provided by the library.


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

This is Cats Effect specific application, meaning it uses Cats IO type under the hood. If you run your program you should see `hello world` printed to the console. Also notice that we are using concrete effect type IO to fill the hole in our `Printer` type, e.g.: `new Printer[IO]` in practice it can be any other effect type like `Task`, although it requires some extra work in the library.
In our example we created `PrinterClient` which does noting but sending `Print` event at the sturtup. In my opinion it doesn't deserve to be a standalone process would be better if we create a process in place:

```scala
object PrinterApp extends CatsApp {
  override def processes: IO[Seq[Process[IO]]] = IO {
    val printer = new Printer[IO]
    val start = Process[IO](_ => {
      case Start => Printer.Print("hello world") ~> printer.ref
    })
    Seq(start, printer)
  }
}
```
Although it's a matter of taste, there is no hard rule.

## DSL

In this chapter I will descibe each dsl operator in details. Let's get started.

### unit

`unit` -  semantically this operator is equivalent with `Monad.unit` and obeys the same lows. Having said that the following expressions are equivalent:

```scala
event ~> process <-> unit ++ event ~> process
event ~> process <-> event ~> process ++ unit
```

This operator can be used in `fold` operator to combine multiple flows. Example:

```scala
processes.map(event ~> _).fold(unit)(_ ++ _)
```

It also can be used to represent an empty flow:

```scala
{
  case Start => unit // do nothing
  case Stop => unit // do nothing
}
```

### flow

`flow` - suspends the thunk that produces a flow. Semantically this operator is equivalent with `suspend` for effects however it's strongly not recommended to perform any side effects within `flow`.

Not recommended:

```scala
def print(str: String) = flow {
  println(str)
  unit
}
```

Recommended:

```scala
def print(str: String) = flow {
  eval(println(str))
}
```

`flow` may be useful to implement recursive flows. Example:

```scala
def times[F[_]](n: Int) = {
  def step(remaining: Int): DslF[F, Unit] = flow {
    if (remaining == 0) unit
    else eval(print(remaining)) ++ step(remaining - 1)
  }

  step(n)
}
```

If you try to remove `flow` you will get `StackOverflowError`

Another useful application is using lazy values inside `flow`. Example:

```scala
  lazy val lazyValue: String = {
    println("evaluated")
    "hello"
  }

  val useLazyValue = flow {
    val tmp = lazyValue + " world"
    eval(println(tmp))
  }
```




TODO

## Channel

TODO

## ParConfig

TODO

