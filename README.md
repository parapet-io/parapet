# Parapet - purely functional library to develop distributed and event driven systems


**Contents**

* [Key Features](#key-features)
* [DSL](#dsl)
* [Process](#process)
* [Channel](#channel)
* [Error Handling and DeadLetterProcess](#error-handling-and-deadletterprocess)
* [Configuration](#configuration)
* [Correctness Properties](#correctness-properties)
* [Distributed Algorithms in Parapet](#distributed-algorithms-in-parapet)
* [Performance Analysis](#performance-analysis)

## Key Features

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

Let's walk through this code. You start writing your processes by extending `Process` trait and parameterizing it with an effect type. In this example we left so called hole `F[_]` in our `Printer` type which can be any type constructor with a single argument, e.g. `F[_]` is a generic type constructor, cats effect `IO` is a specific type constructor and `IO[Unit]` is a concrete type. Starting from this moment it should become clear what it means for a process to be generic. Simply speaking it means that a process doesn't depend on any specific effect type e.g. `IO`. Thus we can claim that our `Printer` process is surely a generic process. The next step is to define a process API or contract that defines set of events that it can send and recieve. Process contract is an important part of any process specification that should be taken seriously. API defines a protocol that other processes will use in order to communicate with your process. Please remember that it's very important aspect of any process definition and take it seriously. The next step would importing `DSL`, Parapet DSL is a small set of operations that we will consider in details in the next chapters, in this example we need only `eval` operator that  suspends a side effect in `F`, in our Printer process we suspend `println` effectful computation. Finally every process should override `handle` function defined in `Process` trait. `handle` function is a partial function that matches input events and produces an executable `flows`. If you ever tried Akka framework you may find this approach familiar (for the curious, `Receive` is simply a type alias for `PartialFunction[Event, DslF[F, Unit]]`). In our Printer process we match on `Print` event using well known pattern-matching feature in Scala language. If you are new in functional programming I'd strongly recommend to read about pattern-matching, it's a very powerfull instrument. 
That's basically it, we consider every important  aspect of our Printer process, let's move forward and write a simple client process that will talk to our Printer.


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

By this moment we have two processes: `Printer` and `PrinterClient`, nice! But wait. We need to run them somehow, right?
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

This chapter descibes each DSL operator in details. Let's get started.

**Contents** 

* [unit](#unit)
* [flow](#flow)
* [send](#send)
* [forward](#forward)
* [par](#par)
* [delay](#delay)
* [withSender](#withSender)
* [fork](#fork)
* [register](#register)
* [race](#race)
* [suspend](#suspend)
* [suspendWith](#suspendWith)
* [eval](#eval)
* [evalWith](#evalWith)

### unit

`unit` -  semantically this operator is equivalent with `Monad.unit` and obeys the same laws. Having said that the following expressions are equivalent:

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

### send

`send` - sends an event to one or more receivers. Event will be delivered to all receivers in the specified order.
Parapet provides a symbolic name for this operator `~>` although in current implentation it doesn't allow to send an event to multiple receivers. It will added in the future releases.

Examples:

```scala
send(Ping, processA, processB, processC)
```

`Ping` event will be sent to the `processA` then `processB` and finaly `processC`. It's not guaranteed that `processA` will receive `Ping` event before `processC` as it depends on it's  processing speed and current workload.


```scala
Ping ~> processA
```

Not supported: 

```scala
Ping ~> Seq(processA, processB, processC)
```

Possible workaround:

```scala
 Seq(processA, processB, processC).map(Ping ~> _).fold(unit)(_ ++ _)
```

Send multiple events to a process:

```scala
Seq(e1, e2, e3) ~> process
```

### forward

`forward` - sends an event to the receiver using original sender reference. This may be useful for implementing a proxy process.
Example:

```scala
val server = Process[IO](_ => {
  case Request(body) => withSender(sender => eval(println(s"$sender-$body")))
})

val proxy = Process[IO](_ => {
  case Request(body) => forward(Request(s"proxy-$body"), server.ref)
})

val client = Process.builder[IO](_ => {
    case Start => Request("ping") ~> proxy
  }).ref(ProcessRef("client")).build
```

The code above will print: `client-proxy-ping`

### par

`par` - executes operations from the given flow in parallel. Example:

```scala
par(eval(print(1)) ++ eval(print(2))) 
```

Possible outputs: `12 or 21`

### delay

`delay` - delays every operation in the given flow for the given duration.
For sequential flows the flowing expressions are semantically equivalent:

```scala
 delay(duration, x~>p ++ y~>p) <-> delay(duration, x~>p) ++ delay(duration, y~>p)
 delay(duration, x~>p ++ y~>p) <-> delay(duration) ++ x~>p ++ delay(duration) ++ y~>p
```

For parallel flows:

```scala
delay(duration, par(x~>p ++ y~>p)) <-> delay(duration) ++ par(x~>p ++ y~>p)
```

Note: since the following flow will be executed in parallel the second operation won't be delayed:

```scala
par(delay(duration) ++ eval(print(1)))
```

instead use

```scala
par(delay(duration, eval(print(1))))
```


### withSender

`withSender` - accepts a callback function that takes a sender reference and produces a new flow. Example:

```scala
val server = Process[IO](_ => {
  case Request(data) => withSender(sender => eval(print(s"$sender says $data")))
})

val client = Process.builder[IO](_ => {
  case Start => Request("hello") ~> server
}).ref(ProcessRef("client")).build
```

The code above will print: `client says hello`

### fork

`fork` - does what exactly the name says, executes the given flow concurrently. Example:

```scala
val process = Process[IO](_ => {
  case Start => fork(eval(print(1))) ++ fork(eval(print(2)))
})
```

Possible outputs: `12 or 21`

### register

`register` - registers a child process in the Parapet context. It's guaranted that a child process will receive `Stop` event before it's parent. Example: 

```scala
  val server = Process[IO](ref => {
    case Start => register(ref, Process[IO](_ => {
      case Stop => eval(println("stop worker"))
    }))
    case Stop => eval(println("stop server"))
  })
```
The code above will print: 

```
stop worker
stop server
```

### race

`race` - runs two flows concurrently. The loser of the race is canceled. 

Example:

```scala
  val forever = eval(while (true) {})

  val process: Process[IO] = Process[IO](_ => {
    case Start => race(forever, eval(println("winner")))
  })
```

Output: `winner`

### suspend

`suspend` - adds an effect which produces `F` to the current flow. Example:

```scala
suspend(IO(print("hello world")))
```

Output: `hello world`

Not recommended:

```scala
suspend {
 println("hello world")
 IO.unit
}
```

### suspendWith

`suspendWith` - suspends an effect which produces `F` and then feeds that into a function that takes a normal value and returns a new flow. All operations from produced flow added to the current flow. Example:

```scala
suspend(IO.pure(1))) { i => eval(print(i)) } 
```

Output: `1`

### eval

`eval` - suspends a side effect in `F` and then adds that to the current flow. Example:

```scala
eval(println("hello world"))
```

Output: `hello world`

### evalWith

`evalWith` - Suspends a side effect in `F` and then feeds that into a function that takes a normal value and returns a new flow. All operations from a produced flow will be added to the current flow. Example:

```scala
evalWith("hello world")(a => eval(println(a)))
```

Output: `hello world`

## Process

`Process` is key abstraction in parapet, any application must have a least one process. If you try to run an application w/o processes you will get an error saying that at least one process required. This section covers some useful features that we haven't seen yet, below you will find a short list of features:

* Predefined processes and reserved references
* Switching process behaviour
* Direct process call
* Process combinators: `and` and `or`
* Testing your processes
* Basic patterns and tips: implementing timeouts, designing API

### Predefined processes and reserved references

Parapet has some reserved process references, e.g.: `KernelRef(parapet-kernel)`, `SystemRef(parapet-system)`, `DeadLetterRef(parapet-deadletter)`, `UndefinedRef(parapet-undefined)`. The general rule is that any reference that starts with `parapet-` prefix can be used by the platform code for any purpose.
Parapet has a `SystemProcess` that cannot be overriden by users. `SystemProcess` is a starting point, i.e. it's created before any other process. Lifecycle event `Start` is sent by `SystemProcess`. Any event sent to the `SystemProcess` will be ignored and dropped. Don't try to send any events to `SystemProcess` b/c it can lead to unpredictable errors, there are  more instersting things to do, believe me.

`DeadLetterProcess` is another process that is created by default, although in can be overriden,  for more details check  `DeadLetterProcess` section under `Event Handling` 

### Switching process behaviour

Sometimes it might be useful to dynamically switch a process behaviour, e.g.: from `uninitialized` to `ready` state. Thankfully `Process` provides `switch` method that does exactly that.

Example:

Lazy server:

```scala
  // for some effect `F[_]`
  val server = new Process[F] {

    val init = eval(println("acquire resources: create socket and etc."))

    def ready: Receive = {
      case Request(data) => withSender(Success(data) ~> _)
      case Stop => eval(println("release resources: close socket and etc."))
    }

    def uninitialized: Receive = {
      case Start => unit // ignore Start event, wait for Init
      case Stop => unit // process is not initialized, do nothing
      case Init => init ++ switch(ready)
      case _ => withSender(Failure("process is not initialized", ErrorCodes.ProcessUninitialized) ~> _)
    }

    override def handle: Receive = uninitialized
  }

  // API

  object Init extends Event

  case class Request(data: Any) extends Event

  sealed trait Response extends Event
  case class Success(data: Any) extends Event
  case class Failure(data: Any, errorCode: Int) extends Event

  object ErrorCodes {
    val ProcessUninitialized  = 0
  }
```

A client which sends `Request` event w/o sending `Init`:

```scala
  val impatientClient = Process[F](_ => {
    case Start => Request("PING") ~> server
    case Success(_) => eval(println("it's not going to happen"))
    case f:Failure => eval(println(f))
  })
```

The code above will print: `Failure(process is not initialized,0)`

A client which sends `Init` first and then `Request`:

```scala
  val humbleClient = Process[F](_ => {
    case Start => Seq(Init, Request("PING")) ~> server
    case Success(data) => eval(println(s"client receive response from server: $data"))
    case _:Failure => eval(println("it's not going to happen"))
  })
```


The code above will print: 

```
acquire resources: create socket and etc.
client receive response from server: PING
release resources: close socket and etc.
```

`switch` is **NOT** thread safe, avoid using `switch` in concurrent flows.

DO NOT do this:

```scala
  val process = new Process[F] {
    def ready: Receive = _

    override def handle: Receive = {
      case Init => fork(switch(ready)) // bad, may lead to inconsistent behaviour
    }
  }

```

If you need to switch a behaviour from a concurrent flow just send an event e.g. `Swith(State.Ready)` to itself. Process will *eventually* switch it's behaviour:

```scala
  val process = new Process[F] {
    def ready: Receive = _

    override def handle: Receive = {
      case Init => fork {
        eval(println("do some work in parallel"))
        Switch(Ready) ~> ref // notify the process that it's time to switch it's behaviour
      }
      case Switch(Ready) =>  switch(ready)
    }
  }

  sealed trait State
  object Ready extends State

  case class Switch(next: State) extends Event
```


### Direct process call

Sometimes it may be useful to call a process directly. Especially it's a common case for short living processes. For instance you may want to create a process, call it and then abandon, garbage collector will do it's job. However if you try to send an enent to a process that doesn't exist in system you will receive `Failure` event with `UnknownProcessException`. This is where `direct  call` comes to rescue.

Example:

```scala
  // API
  case class Sum(a: Int, b: Int) extends Event
  case class Result(value: Int) extends Event

  class Calculator[F[_]] extends Process[F] {
    override def handle: Receive = {
      case Sum(a, b) => withSender(Result(a + b - 1) ~> _) // yes, very poor calculator
    }
  }

  val student = Process[F](ref => {
    case Start => new Calculator().apply(ref, Sum(2, 2))
    case Result(value) => eval(println(s"2 + 2 = $value"))
  })

```

Output: `2 + 2 = 3`

Note that `apply` method doesn't return a normal value rather it returns a *program* which will be executed as a normal flow. 
In other words the following expressions are equivalent:

```
Sum(2, 2) ~> calculator <-> new Calculator().apply(ref, Sum(2, 2)) // where ref belongs to the same process in both cases
```

### Process combinators

### Testing your processes

### Basic patterns and tips



TODO

## Channel

Channel is a process that implements strictly synchronous request-reply dialog. The channel sends an event to a receiver and then waits for a response in one step, i.e. it blocks asynchronously until it receives a response. Doing any other sequence, e.g., sending two request or reply events in a row will result in a failure returned to the sender.

Example for some `F[_]`: 

```scala
  val server = new Process[F] {
    override def handle: Receive = {
      case Request(data) => withSender(sender => Response(s"echo: $data") ~> sender)
    }
  }

  val client = new Process[F] {

    lazy val ch = Channel[F]

    override def handle: Receive = {
      case Start => register(ref, ch) ++
        ch.send(Request("PING"), server.ref, {
          case scala.util.Success(Response(data)) => eval(println(data))
          case scala.util.Failure(err) => eval(println(s"server failed to process request. err: ${err.getMessage}"))
        })

    }
  }


  case class Request(data: Any) extends Event

  case class Response(data: Any) extends Event
```

## Error Handling and DeadLetterProcess

There are some scenarions when a process may receive a `Failure` event:

* When a target process failed to handle an event sent by another process.

Example: 

```scala

  // for some effect F[_]
  val faultyServer = Process.builder[F](_ => {
    case Request(_) => eval(throw new RuntimeException("server is down"))
  }).ref(ProcessRef("server")).build

  val client = Process.builder[F](_ => {
    case Start => Request("PING") ~> faultyServer
    case Failure(Envelope(me, event, receiver), EventHandlingException(errMsg, cause)) => eval {
      println(s"self: $me")
      println(s"event: $event")
      println(s"receiver: $receiver")
      println(s"errMsg: $errMsg")
      println(s"cause: ${cause.getMessage}")
    }
  }).ref(ProcessRef("client")).build
```

The code above will output: 

```
self: client
event: Request(PING)
receiver: server
errMsg: process [name=undefined, ref=server] has failed to handle event: Request(PING)
cause: server is down
```

`EventHandlingException` indicates that a receiver process failed to handle an event.

* When a process event queue is full. It's possible when a process experiencing performance degradation due to heavy load.

Example:

For this example we need to tweak SchedulerConfig: 

```
queueSize = 10000
processQueueSize = 100
```

```scala
  // for some effect F[_]
  val slowServer = Process.builder[F](_ => {
    case Request(_) => eval(while (true) {}) // very slow process...
  }).ref(ProcessRef("server")).build

  val client = Process.builder[F](_ => {
    case Start =>
      generateRequests(1000) ~> slowServer
    case Failure(Envelope(me, event, receiver), EventDeliveryException(errMsg, cause)) => eval {
      println(s"self: $me")
      println(s"event: $event")
      println(s"receiver: $receiver")
      println(s"errMsg: $errMsg")
      println(s"cause: ${cause.getMessage}")
      println("=====================================================")
    }
  }).ref(ProcessRef("client")).build

  def generateRequests(n: Int): Seq[Event] = {
    (0 until n).map(Request)
  }
```

The code above will print a dozens of lines, four lines per `Failure` event:

```
client sent events
self: client
event: Request(101)
receiver: server
errMsg: System failed to deliver an event to process [name=undefined, ref=server]
cause: process [name=undefined, ref=server] event queue is full
=====================================================
self: client
event: Request(102)
receiver: server
errMsg: System failed to deliver an event to process [name=undefined, ref=server]
cause: process [name=undefined, ref=server] event queue is full
=====================================================
self: client
event: Request(103)
receiver: server
errMsg: System failed to deliver an event to process [name=undefined, ref=server]
cause: process [name=undefined, ref=server] event queue is full
=====================================================
```

`EventDeliveryException` indicates that system failed to deliver an event. Handling such types of errors may be useful for runtime analysis, e.g. a sender process might consider to lower event send rate or even stop sending events to let a target process to finish processing pending events. It's worth noting that you should avoid any long running computations when processing `Failure` events because it could lead to *cascading failures*.

* A process event handler isn't defind for some events.

Example:

```scala
  // for some effect F[_]
  val uselessService = Process.builder[F](_ => {
    case Start => unit
    case Stop => unit
  }).ref(ProcessRef("server")).build

  val client = Process.builder[F](_ => {
    case Start =>
      Request("PING") ~> uselessService
    case Failure(Envelope(me, event, receiver), EventMatchException(errMsg)) => eval {
      println(s"self: $me")
      println(s"event: $event")
      println(s"receiver: $receiver")
      println(s"errMsg: $errMsg")
    }
  }).ref(ProcessRef("client")).build
```

The code above will print:

```
self: client
event: Request(PING)
receiver: server
errMsg: process [name=undefined, ref=server] handler is not defined for event: Request(PING)
```

* A process doesn't exist in Parapet system.

Example:

```scala
  // for some effect F[_]
  val unknownService = Process.builder[F](_ => {
    case Start => unit
    case Stop => unit
  }).ref(ProcessRef("server")).build

  val client = Process.builder[F](_ => {
    case Start =>
      Request("PING") ~> unknownService
    case Failure(Envelope(me, event, receiver), UnknownProcessException(errMsg)) => eval {
      println(s"self: $me")
      println(s"event: $event")
      println(s"receiver: $receiver")
      println(s"errMsg: $errMsg")
    }
  }).ref(ProcessRef("client")).build
```

The code above will print:

```
self: client
event: Request(PING)
receiver: server
errMsg: there is no such process with id=server registered in the system
```

Final notes regarding error handling:

* All `Failure` events sent by `parapet-system` process (if you are curious you can check it by yourself using `withSender`).
* If a process has no error handling then `Failure` event  will be send to `DeadLetterProcess`. More about `DeadLetterProcess` you will  find below

### DeadLetterProcess

The library by default provides an implementation of `DeadLetterProcess` which just logs failures. Although it might be not very practical, for instance you may prefer to store failures into a database for further analyses. The library allows to provide a custom implementation of `DeadLetterProcess`.

Example using `CatsApp`:

```scala
import cats.effect.IO
import io.parapet.CatsApp
import io.parapet.core.Event.{DeadLetter, Start}
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.core.{Event, Process, ProcessRef}


object CustomDeadLetterProcessDemo extends CatsApp {

  import dsl._

  override def deadLetter: IO[DeadLetterProcess[IO]] = IO.pure {
    new DeadLetterProcess[IO] {
      override def handle: Receive = {
        // can be stored in database
        case DeadLetter(envelope, error) => eval {
          println(s"sender: ${envelope.sender}")
          println(s"receiver: ${envelope.receiver}")
          println(s"event: ${envelope.event}")
          println(s"errorType: ${error.getClass.getSimpleName}")
          println(s"errorMsg: ${error.getMessage}")
        }
      }
    }
  }

  val faultyServer = Process.builder[IO](_ => {
    case Request(_) => eval(throw new RuntimeException("server is down"))
  }).ref(ProcessRef("server")).build

  val client = Process.builder[IO](_ => {
    case Start => Request("PING") ~> faultyServer
    // no error handling
  }).ref(ProcessRef("client")).build

  override def processes: IO[Seq[Process[IO]]] = IO {
    Seq(client, faultyServer)
  }

  case class Request(data: Any) extends Event

}
```

The code above will print:

```
sender: client
receiver: server
event: Request(PING)
errorType: EventHandlingException
errorMsg: process [name=undefined, ref=server] has failed to handle event: Request(PING)
```


## Configuration

TODO

## Correctness Properties

TODO

## Distributed Algorithms in Parapet

TODO

## Performance Analysis

## License

```
   Copyright [2019] The Parapet Project Developers

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0
```

`∏åRÂπ∑†`
