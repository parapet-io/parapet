# Parapet

**A purely functional Scala 3 toolkit for building distributed systems.**

Parapet lets you describe a distributed system as a composable program - a value - and then
interpret that program into a running system with scheduling, messaging, and supervision.

Each process is an isolated unit with a mailbox. Its handler is an ordinary value in a typed
DSL, executed by the runtime against any compatible effect type.

```scala
final case class Ping(n: Int) extends Event
final case class Pong(n: Int) extends Event

class Echo[F[_]](peer: ProcessRef[Ping]) extends Process[F, Ping | Pong]:
  import dsl.*
  override def handle: Receive =
    case Start                   => Ping(0) ~> peer            // kick things off
    case Ping(n) if n < 3        => reply(Pong(n + 1))         // reply to sender
    case Pong(n)                 => eval(println(s"got $n")) ++
                                    (Ping(n) ~> peer)          // sequential DSL
    case Failure(_, _, _, error) => eval(println(error))       // failures are events
```

**Documentation, guides, and API reference: [parapet.io](https://parapet.io)**

## Why Parapet

* **Programs are values.** A handler is an ordinary value (`DslF[F, Unit]`) you can store,
  compose with `++` / `par` / `race`, inspect, and test deterministically without spinning up
  a real distributed system. Internally: a `Free` algebra over a small set of operations.
* **Pure actor semantics without an actor system.** Each process owns one mailbox, sees
  messages sequentially, and never shares mutable state. No `Props`, no `ActorRef` casting -
  just process refs and the `~>` operator.
* **Effect-system agnostic.** Handlers are generic over `F[_]`. Use a backend integration such
  as `parapet-cats-effect`, or provide your own `Effect[F]`, `Parallel[F]`, and scheduler
  runtime instances.
* **Batteries for distribution.** Pluggable transports (`parapet-net`), protobuf `WireCodec`
  (`parapet-protocol`), an integration test-kit, bounded mailboxes, first-class failure
  events, and overridable dead-letter hooks.

## Modules

| Module                  | Purpose                                                                    |
| ----------------------- | -------------------------------------------------------------------------- |
| `parapet-core`          | Process model, DSL, scheduler, and effect-polymorphic capability contracts |
| `parapet-testkit`       | Shared conformance specs for backend/runtime implementations               |
| `parapet-cats-effect`   | Recommended production backend for Cats Effect `IO`                        |
| `parapet-pario`         | Small reference runtime for examples, tests, and learning                  |
| `parapet-protocol`      | `WireCodec`, protobuf message definitions, and wire command vocabulary     |
| `parapet-net`           | TCP / UDP transports and adapter processes                                 |

## Getting started

`parapet-core` is the only dependency you need to define processes. To run an application, add
a backend runtime such as `parapet-cats-effect`.

```scala
libraryDependencies += "io.parapet" %% "parapet-core"        % version
libraryDependencies += "io.parapet" %% "parapet-cats-effect" % version
```

A process declares the events it accepts and reacts to them inside `handle`:

```scala
import io.parapet.{Event, Process, ProcessRef}
import io.parapet.Event.Start

class Printer[F[_]] extends Process[F, Printer.Print]:
  import Printer.*
  import dsl.*

  override def handle: Receive =
    case Print(data) => eval(println(data))

object Printer:
  final case class Print(data: Any) extends Event

class Greeter[F[_]](printer: ProcessRef[Printer.Print]) extends Process[F, Event]:
  import dsl.*

  override def handle: Receive =
    case Start => Printer.Print("hello world") ~> printer
```

`Start` and `Stop` are lifecycle events delivered by the runtime. `ProcessRef[In]` is the
address of a process and states which events it accepts - prefer it over passing `Process`
instances around, so wiring stays late-binding.

Processes are generic in `F[_]` and specialized at the application boundary:

```scala
import cats.effect.IO
import io.parapet.Process
import io.parapet.cats.CatsEffectParApp

object HelloApp extends CatsEffectParApp:
  def processes(args: Array[String]): IO[Seq[Process[IO, ?]]] =
    IO.delay {
      val printer = new Printer[IO]
      val greeter = new Greeter[IO](printer.ref)
      Seq(printer, greeter)
    }
```

Run it like any other JVM `@main` and you'll see `hello world` on stdout. The same processes
run on a different effect runtime by supplying your own `ParApp[F]`.

The DSL reference, request/reply channels, failure handling, scheduler configuration, and the
transport modules are documented at **[parapet.io](https://parapet.io)**.

## How it differs

* **vs actor frameworks (Akka, etc.)** - similar process/mailbox model, but handlers are
  programs (values) in a typed DSL. They are composable, inspectable, and re-interpretable
  rather than directly executed behaviours.
* **vs effect systems (Cats Effect, ZIO)** - builds on an effect runtime and adds a structured
  model of distributed processes: mailboxes, message passing, supervision.
* **Same program, multiple interpreters.** A handler runs unchanged in production and under a
  deterministic test interpreter - no test-mode fork of your code, no separate simulation
  framework.

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
