# Parapet

**A purely functional Scala 3 toolkit for building distributed systems.**

Parapet lets you describe a distributed system as a composable program - a value -
and then interpret that program into a running system with scheduling, messaging, and
supervision.

Each process is an isolated unit with a mailbox. Its handler is an ordinary value in a
typed DSL, executed by the runtime against any compatible effect type.

```scala
final case class Ping(n: Int) extends Event
final case class Pong(n: Int) extends Event

class Echo[F[_]](peer: ProcessRef[Ping]) extends Process[F, Ping | Pong]:
  import dsl.*
  override def handle: Receive =
    case Start              => Ping(0) ~> peer              // kick things off
    case Ping(n) if n < 3   => reply(Pong(n + 1))           // reply to sender
    case Pong(n)            => eval(println(s"got $n")) ++
                               (Ping(n) ~> peer)            // sequential DSL
    case Failure(_, cause)  => eval(println(cause))         // failures are events
```

## What Parapet is for

Parapet is a library of primitives for building distributed systems. It gives you the
substrate - processes, mailboxes, a typed DSL, a scheduler, pluggable transports, a
wire-codec type-class - and trusts you to compose it into whatever topology your problem
demands.

The toolkit is still filling out. Higher-level building blocks that production systems
depend on - reliable channels, failure detectors, broadcast, gossip, CRDTs, sharding,
observability - are being added as standalone modules.

Typical use cases:

* **Consensus and coordination protocols** - Raft, Paxos, leader election, membership,
  failure detectors.
* **Replication, broadcast, and streaming systems** built on top of your own wire
  protocol or transport.
* **Deterministic testing of distributed algorithms** - every handler is a value, so
  you can step, replay, and assert on full execution traces under a test interpreter
  before the code ever hits the network.

## Why Parapet

* **Programs are values.** A handler is an ordinary value (`DslF[F, Unit]`) you can
  store, compose with `++` / `par` / `race`, inspect, and test deterministically
  without spinning up a real distributed system. Internally: a `Free` algebra over a
  small set of operations.
* **Pure actor semantics without an actor system.** Each process owns one mailbox, sees
  messages sequentially, and never shares mutable state. No `Props`, no `ActorRef`
  casting - just process refs and the `~>` operator.
* **Effect-system agnostic.** Handlers are generic over `F[_]`. Parapet core defines
  the framework and required capabilities, but it does not provide or require a
  production effect runtime. Use a backend integration such as `parapet-cats-effect`,
  or provide your own `Effect[F]`, `Parallel[F]`, and scheduler runtime instances.
* **Composable handlers.** Combine partial behaviours with `and` / `or`, swap them
  at runtime with `switch`, compose supervised children with `register`.
* **Batteries for distribution.** Pluggable transports (`parapet-net`), protobuf `WireCodec`
  (`parapet-protocol`), and an integration test-kit.
* **Predictable scheduling model.** Bounded per-process mailboxes, a configurable
  scheduler, first-class failure events delivered to senders, overridable dead-letter
  and event-log hooks.

## Modules

| Module                  | Purpose                                                                    |
| ----------------------- | -------------------------------------------------------------------------- |
| `parapet-core`          | Process model, DSL, scheduler, and effect-polymorphic capability contracts |
| `parapet-testkit`       | Shared conformance specs for backend/runtime implementations               |
| `parapet-cats-effect`   | Recommended production backend for Cats Effect `IO`                        |
| `parapet-pario`         | Small reference runtime for examples, tests, and learning                  |
| `parapet-protocol`      | `WireCodec`, protobuf message definitions, and wire command vocabulary     |
| `parapet-net`           | TCP / UDP transports and adapter processes                                 |

## Contents

* [Getting started](#getting-started)
* [Defining a process](#defining-a-process)
* [Typed process refs](#typed-process-refs)
* [Running an application](#running-an-application)
* [DSL cheatsheet](#dsl-cheatsheet)
* [Channel - request/response](#channel--requestresponse)
* [Error handling](#error-handling)
* [Configuration](#configuration)
* [Contributing](#contributing)
* [License](#license)

## Getting started

`parapet-core` is the only dependency you need to define processes. To run an application, add a backend runtime such as
`parapet-cats-effect`.

```scala
libraryDependencies += "io.parapet" %% "parapet-core" % version
libraryDependencies += "io.parapet" %% "parapet-cats-effect" % version
```

Optional modules:

```scala
libraryDependencies += "io.parapet" %% "parapet-pario"      % version
libraryDependencies += "io.parapet" %% "parapet-protocol"   % version
libraryDependencies += "io.parapet" %% "parapet-net"        % version
```

## Defining a process

Processes are defined generically over the effect type `F[_]`, then specialized at the
application boundary. A process declares its API as events and reacts to them inside
`handle`.

```scala
import io.parapet.{Event, ProcessRef}
import io.parapet.core.Process

class Printer[F[_]] extends Process[F, Printer.Print]:
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

class Greeter[F[_]](printer: ProcessRef[Printer.Print]) extends Process[F, Event]:
  import dsl.*
  override def handle: Receive =
    case Start => Printer.Print("hello world") ~> printer
```

Notes:

* `Start` and `Stop` are lifecycle events delivered by the runtime - `Start` once on
  registration, `Stop` (or `Kill`) on shutdown.
* `ProcessRef` is the address of a process; prefer it over passing `Process` instances
  around so that wiring stays late-binding.

## Typed process refs

Declare a process protocol with `Process[F, MyEvent]`, then pass around `ProcessRef[MyEvent]` to keep wiring explicit.

```scala
final case class Save(key: String, value: String) extends Event

class Store[F[_]] extends Process[F, Save]:
  import dsl.*
  override def handle: Receive =
    case Save(key, value) => eval(println(s"saved $key=$value"))
    case Stop             => eval(println("closing store"))

class Writer[F[_]](store: ProcessRef[Save]) extends Process[F, Event]:
  import dsl.*
  override def handle: Receive =
    case Start => Save("hello", "world") ~> store
```

## Running an application

Specialize your application against a backend at the application boundary. For production Scala FP code, the recommended
backend is Cats Effect:

```scala
import cats.effect.IO
import io.parapet.cats.CatsEffectParApp
import io.parapet.core.Process

object HelloApp extends CatsEffectParApp:
  def processes(args: Array[String]): IO[Seq[Process[IO, ?]]] =
    IO.delay {
      val printer = new Printer[IO]
      val greeter = new Greeter[IO](printer.ref)
      Seq(printer, greeter)
    }
```

Run it like any other JVM `@main` and you'll see `hello world` on stdout. Because
`Printer` and `Greeter` are generic in `F`, the same code can target a different effect
runtime by providing your own `ParApp[F]` subclass with the required core capability instances.

## DSL cheatsheet

`dsl._` brings the program combinators into scope inside a process. Most real programs
only need a handful:

| Combinator       | Description                                         |
| ---------------- | --------------------------------------------------- |
| `event ~> ref`   | Send an event to a process.                         |
| `reply(event)`   | Send an event back to the current sender.           |
| `eval { ... }`   | Run a side-effecting computation in `F`.            |
| `p1 ++ p2`       | Sequential composition.                             |
| `par(p1, p2)`    | Run two programs concurrently.                      |
| `race(p1, p2)`   | Run both, keep the first to finish.                 |

The full set of combinators:

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

def server[F[_]]: Process[F, Request] = Process.typed[F, Request](_ => {
  case Request(data) => reply(Response(s"echo: $data"))
})

class Client[F[_]: Effect](backend: ProcessRef[Request]) extends Process[F, Event]:
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

val faulty = Process.typedBuilder[F, Request](_ => {
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

object MyApp extends CatsEffectParApp:
  override val config: ParConfig = ParConfig.default
    .withSchedulerConfig(_.copy(numberOfWorkers = 8, queueSize = 64 * 1024))
  ...
```

`SchedulerConfig` knobs:

* `numberOfWorkers` - worker thread count (defaults to available processors).
* `queueSize` - bound on the shared task queue.
* `processQueueSize` - per-process mailbox bound; `-1` means unbounded.

When a mailbox overflows, events are routed to `EventLog` (a no-op stub by default -
override `eventLog` to persist).

## How it differs

* **vs actor frameworks (Akka, etc.)** - similar process/mailbox model, but handlers are
  programs (values) in a typed DSL. They are composable, inspectable, and
  re-interpretable rather than directly executed behaviours.

* **vs effect systems (Cats Effect, ZIO)** - builds on an effect runtime and adds a
  structured model of distributed processes: mailboxes, message passing, supervision.

* **Same program, multiple interpreters.** A handler runs unchanged in production and
  under a deterministic test interpreter - no test-mode fork of your code, no separate
  simulation framework.

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
