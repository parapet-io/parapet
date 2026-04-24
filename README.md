# Parapet

A purely functional Scala 3 library for building distributed and event-driven systems.

Parapet plays the role of an execution framework for distributed algorithms - it sits
between a low-level effect runtime and high-level process logic. Distributed engineers
focus on algorithms; the runtime handles mailboxes, scheduling, lifecycle, and
back-pressure.

## Modules

| Module          | Purpose                                                                 |
| --------------- | ----------------------------------------------------------------------- |
| `core`          | Process model, DSL, scheduler, `ParIO` effect type, `ParApp`/`ParIOApp` |
| `protocol`      | `WireCodec` type-class and protobuf message definitions                 |
| `net`           | TCP / UDP transports and adapter processes                              |
| `raft`          | Raft consensus (election, replication, commit) as a `Process`           |
| `demo-coloring` | Distributed graph-coloring demo with a 3D web UI                        |
| `intg-tests`    | Cross-module integration test suite                                     |

## Contents

* [Key features](#key-features)
* [Getting started](#getting-started)
* [Defining a process](#defining-a-process)
* [Running an application](#running-an-application)
* [DSL cheatsheet](#dsl-cheatsheet)
* [Channel - request/response](#channel--requestresponse)
* [Error handling](#error-handling)
* [Configuration](#configuration)
* [Contributing](#contributing)
* [License](#license)

## Key features

* Purely functional - Tagless-Final and Free-monad encoded DSL; programs are values.
* Single-mailbox actor model - sequential delivery, no shared mutable state.
* Modular runtime - `Effect[F]` / `Parallel[F]` type-classes let you swap effect types;
  `ParIO` is shipped out of the box.
* Composable processes - combine handlers with `and` / `or`, swap state with `switch`.
* Lightweight scheduler - bounded mailboxes, configurable worker pool, optional
  `EventLog` overflow.

## Getting started

`parapet-core` is the only dependency you need to define and run processes.

```scala
libraryDependencies += "io.parapet" %% "parapet-core" % version
```

Optional modules:

```scala
libraryDependencies += "io.parapet" %% "parapet-protocol" % version
libraryDependencies += "io.parapet" %% "parapet-net"      % version
libraryDependencies += "io.parapet" %% "parapet-raft"     % version
```

## Defining a process

Processes are defined generically over the effect type `F[_]`, then specialized at the
application boundary. A process declares its API as events and reacts to them inside
`handle`.

```scala
import io.parapet.{Event, ProcessRef}
import io.parapet.core.Process

class Printer[F[_]] extends Process[F]:
  import Printer.*
  import dsl.*

  override def handle: Receive =
    case Print(data) => eval(println(data))

object Printer:
  final case class Print(data: Any) extends Event
```

A second process talks to the printer using the `~>` (send) operator:

```scala
import io.parapet.ProcessRef
import io.parapet.core.Process
import io.parapet.core.Events.Start

class Greeter[F[_]](printer: ProcessRef) extends Process[F]:
  import dsl.*
  override def handle: Receive =
    case Start => Printer.Print("hello world") ~> printer
```

Notes:

* `Start` and `Stop` are lifecycle events delivered by the runtime - `Start` once on
  registration, `Stop` (or `Kill`) on shutdown.
* `ProcessRef` is the address of a process; prefer it over passing `Process` instances
  around so that wiring stays late-binding.

## Running an application

Specialize your application against `ParIO`, parapet's bundled effect type, by
extending [`ParIOApp`](core/src/main/scala/io/parapet/ParIOApp.scala):

```scala
import io.parapet.ParIOApp
import io.parapet.core.Process
import io.parapet.effect.ParIO

object HelloApp extends ParIOApp:
  def processes(args: Array[String]): ParIO[Seq[Process[ParIO]]] =
    ParIO.delay {
      val printer = new Printer[ParIO]
      val greeter = new Greeter[ParIO](printer.ref)
      Seq(printer, greeter)
    }
```

Run it like any other JVM `@main` and you'll see `hello world` on stdout. Because
`Printer` and `Greeter` are generic in `F`, the same code can target a different effect
runtime by providing your own `ParApp[F]` subclass with `Effect[F]` / `Parallel[F]`
instances.

## DSL cheatsheet

`dsl._` brings the program combinators into scope inside a process:

| Combinator                 | Description                                              |
| -------------------------- | -------------------------------------------------------- |
| `unit`                     | The empty program - useful as a fold seed.               |
| `event ~> ref`             | Send `event` to a process.                               |
| `forward(event, ref)`      | Send while preserving the original sender.               |
| `reply(event)`             | Send `event` back to the sender of the current message.  |
| `withSender(s => program)` | Run a program parameterised by the current sender ref.   |
| `flow { ... }`             | Suspend program construction (use for recursion).        |
| `eval { sideEffect }`      | Suspend a side-effecting computation in `F`.             |
| `evalWith(value)(f)`       | Evaluate `value` and feed it into a program builder.     |
| `suspend(fa)`              | Lift a value of `F[A]` into the program.                 |
| `delay(d, program)`        | Defer every step in `program` by duration `d`.           |
| `par(program)`             | Run independent steps of `program` in parallel.          |
| `fork(program)`            | Fire-and-forget concurrent execution.                    |
| `race(p1, p2)`             | Run both, cancel the loser.                              |
| `register(parent, child)`  | Register a child process; child receives `Stop` first.   |
| `switch(receive)`          | Replace the current process behaviour.                   |

Combine programs with `++` (sequential composition) - e.g. `eval(println("a")) ++ eval(println("b"))`.

## Channel - request/response

`Channel` turns the asynchronous mailbox model into a strict one-call-at-a-time
request/reply dialog. Send through it, get a `Try[Event]` back.

```scala
import io.parapet.{Event, ProcessRef}
import io.parapet.core.{Channel, Process}
import io.parapet.core.Events.Start
import io.parapet.effect.Effect

final case class Request(data: String) extends Event
final case class Response(data: String) extends Event

def server[F[_]]: Process[F] = Process[F](_ => {
  case Request(data) => reply(Response(s"echo: $data"))
})

class Client[F[_]: Effect](backend: ProcessRef) extends Process[F]:
  import dsl.*
  private lazy val ch = Channel[F]()

  override def handle: Receive =
    case Start =>
      register(ref, ch) ++
        ch.send(Request("PING"), backend, {
          case scala.util.Success(Response(d)) => eval(println(d))
          case scala.util.Failure(err)         => eval(println(s"failed: ${err.getMessage}"))
        })
```

## Error handling

Failures are first-class events. Every problem is reported via a `Failure` event
delivered to the original sender (or, if the sender has no handler for it, the
configured `DeadLetterProcess`).

```scala
import io.parapet.core.Envelope
import io.parapet.core.Events.{Failure, Start}
import io.parapet.core.exceptions.EventHandlingException
import io.parapet.core.Process

val faulty = Process.builder[F](_ => {
  case Request(_) => eval(throw new RuntimeException("server is down"))
}).ref(ProcessRef("server")).build

val client = Process.builder[F](_ => {
  case Start => Request("PING") ~> faulty
  case Failure(Envelope(self, event, receiver), EventHandlingException(msg, cause)) =>
    eval(println(s"$receiver failed: $msg (cause: ${cause.getMessage})"))
}).ref(ProcessRef("client")).build
```

Common failure causes:

* `EventHandlingException` - receiver's handler threw.
* `EventDeliveryException` - receiver's mailbox is full.
* `EventMatchException` - receiver's handler is not defined for the event.
* `UnknownProcessException` - receiver isn't registered.

Override `ParApp#deadLetter` to install a custom `DeadLetterProcess` (e.g. one that
persists or alerts on dropped events).

## Configuration

Override `config` on your `ParApp` to tune the scheduler:

```scala
import io.parapet.core.Parapet.ParConfig

object MyApp extends ParIOApp:
  override val config: ParConfig = ParConfig.default
    .withSchedulerConfig(_.copy(numberOfWorkers = 8, queueSize = 1 << 16))
  ...
```

`SchedulerConfig` knobs:

* `numberOfWorkers` - worker thread count (defaults to available processors).
* `queueSize` - bound on the shared task queue.
* `processQueueSize` - per-process mailbox bound; `-1` means unbounded.

When a mailbox overflows, events are routed to `EventLog` (a no-op stub by default -
override `eventLog` to persist).

## Contributing

The project is in active development. Issues, ideas, and pull requests are welcome.

## License

```
Copyright 2019 The Parapet Project Developers

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
```
