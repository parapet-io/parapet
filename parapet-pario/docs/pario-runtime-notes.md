# ParIO Runtime Notes

This note explains what `ParIO` is, how the current runloop works, and which external sources are worth studying.

ParIO lives in `parapet-pario` as a reference runtime. It is useful for examples, tests, learning, and runtime
experimentation. It is not the recommended production backend and is not intended to replace Cats Effect, ZIO, or
Reactor.

The code paths discussed here are:

- [ParIO.scala](/Users/dmgcodevil/dev/parapet/parapet-pario/src/main/scala/io/parapet/effect/ParIO.scala)
- [ParIORuntime.scala](/Users/dmgcodevil/dev/parapet/parapet-pario/src/main/scala/io/parapet/effect/ParIORuntime.scala)
- [Effect.scala](/Users/dmgcodevil/dev/parapet/parapet-core/src/main/scala/io/parapet/effect/Effect.scala)
- [ParIOApp.scala](/Users/dmgcodevil/dev/parapet/parapet-pario/src/main/scala/io/parapet/ParIOApp.scala)
- [DslInterpreter.scala](/Users/dmgcodevil/dev/parapet/parapet-core/src/main/scala/io/parapet/core/DslInterpreter.scala)

## The three main concepts

### 1. `ParIO[A]`

`ParIO[A]` is just the program data type.

It is an algebra of nodes like:

- `Pure`
- `Delay`
- `Blocking`
- `Suspend`
- `FlatMap`
- `HandleError`
- `Sleep`

Nothing executes when these values are created. They are just a tree/program.

### 2. `Effect[F]`

`Effect[F]` is the capability contract parapet needs from any effect runtime.

That is why the scheduler and interpreter are generic in `F[_]`.

If we can provide:

- `given Effect[IO]`
- `given Parallel[IO]`

then parapet core can run on Cats Effect `IO`, `ParIO`, or some other effect type.

So:

- `ParIO` is one concrete effect type
- `Effect` is the abstraction boundary core code programs against

### 3. `ParIORuntime`

`ParIORuntime` is the interpreter/executor side for `ParIO`.

This is where the thread pools live:

- parallel pool
- async pool
- blocking pool
- timer executor

The default blocking pool is intentionally elastic with zero core threads and an effectively unbounded maximum. That is
not because "unbounded is ideal", but because the current synchronous runloop still uses the blocking context for waits
such as queue take, deferred get, sleep, and fiber join.

This is also where the concrete `given Effect[ParIO]` and `given Parallel[ParIO]` now live.

That separation is important:

- `ParIO` stays a pure data type
- `ParIORuntime` becomes configurable

## How the runloop works

The heart of the runtime is:

```scala
var current: ParIO[Any] = io.asInstanceOf[ParIO[Any]]
var stack: List[Frame]  = Nil
```

This is the trampoline.

### `current`

`current` is the node the interpreter is evaluating right now.

Examples:

- `Pure(42)`
- `Delay(() => println("x"))`
- `FlatMap(source, bind)`

The runloop repeatedly pattern-matches on `current` and decides what the next step is.

### `stack`

`stack` is not the JVM call stack.

It is a heap-allocated continuation stack used by the interpreter so we do not recurse through `flatMap` chains with
ordinary method calls.

This is the key stack-safety trick.

### `Frame`

A `Frame` is one pending continuation.

Current frames are:

- `BindFrame(run: Any => ParIO[Any])`
- `RecoverFrame(run: Throwable => ParIO[Any])`

So:

- a `BindFrame` means "when you produce a value, continue with this `flatMap` function"
- a `RecoverFrame` means "if something throws, recover with this handler"

## Tiny mental model

For a program like:

```scala
val io =
  ParIO.pure(1)
    .flatMap(n => ParIO.pure(n + 1))
    .flatMap(n => ParIO.delay(n * 10))
```

the runtime first sees a nested program tree:

```scala
FlatMap(
  FlatMap(Pure(1), f1),
  f2
)

where
f1 = n => ParIO.pure(n + 1)
f2 = n => ParIO.delay(n * 10)
```

In the tables below, the top row is the top of the continuation stack.

### Step 0

`current = FlatMap(FlatMap(Pure(1), f1), f2)`

| Continuation stack |
| --- |
| _(empty)_ |

### Step 1

The outer `FlatMap` pushes `f2`, then moves into its source.

`current = FlatMap(Pure(1), f1)`

| Continuation stack |
| --- |
| `BindFrame(f2)` |

### Step 2

The inner `FlatMap` pushes `f1`, then moves into `Pure(1)`.

`current = Pure(1)`

| Continuation stack |
| --- |
| `BindFrame(f1)` |
| `BindFrame(f2)` |

### Step 3

`Pure(1)` pops `BindFrame(f1)` and computes the next node.

`current = Pure(2)`

| Continuation stack |
| --- |
| `BindFrame(f2)` |

### Step 4

`Pure(2)` pops `BindFrame(f2)` and computes the final delayed node.

`current = Delay(() => 20)`

| Continuation stack |
| --- |
| _(empty)_ |

### Step 5

`Delay(() => 20)` runs its thunk and becomes `Pure(20)`.

`current = Pure(20)`

| Continuation stack |
| --- |
| _(empty)_ |

When the stack is empty and `current` is `Pure(value)`, evaluation is done.

## Why this is stack-safe

The important property is:

- `unsafeRunLoop` uses a `while` loop
- `flatMap` continuations are stored in `stack`
- the JVM call stack does not grow with every bind

So a deeply nested `flatMap` chain becomes iterative evaluation over heap data.

This is the same family of idea described in the trampoline/free-monad literature.

## Error handling

Errors are handled by scanning `stack` for the nearest `RecoverFrame`.

For example:

```scala
val boom = new RuntimeException("boom")

val io =
  ParIO.raiseError[Int](boom)
    .flatMap(n => ParIO.pure(n + 1))
    .handleErrorWith(_ => ParIO.pure(123))
    .flatMap(n => ParIO.pure(n * 2))
```

The decomposed shape is:

```scala
FlatMap(
  HandleError(
    FlatMap(Delay(() => throw boom), f1),
    h
  ),
  f2
)

where
f1 = n => ParIO.pure(n + 1)
h  = _ => ParIO.pure(123)
f2 = n => ParIO.pure(n * 2)
```

Right before the error is raised, the stack looks like this:

`current = Delay(() => throw boom)`

| Continuation stack |
| --- |
| `BindFrame(f1)` |
| `RecoverFrame(h)` |
| `BindFrame(f2)` |

When `Delay` throws, the catch block scans from the top:

1. it skips `BindFrame(f1)` because that continuation only applies on success
2. it finds `RecoverFrame(h)`
3. it sets `current = h(error)`, which becomes `Pure(123)`
4. it drops the handled prefix and keeps the tail below the handler

After recovery starts, the stack is:

`current = Pure(123)`

| Continuation stack |
| --- |
| `BindFrame(f2)` |

So the continuation stack carries both success continuations and error continuations, and recovery resumes with whatever
work was still below the matching handler.

## Current concurrency model

### `start`

`Effect.start(fa)` submits `fa` to the runtime's async pool and returns an `EffectFiber`.

This is the ordinary background/concurrent context.

### `startBlocking`

`Effect.startBlocking(fa)` submits `fa` to the blocking pool.

This is what parapet `offload(...)` uses now.

That is an important semantic split:

- `fork` -> async context
- `offload` -> blocking context

### `blocking`

`ParIO.blocking(thunk)` means:

- if we are already in the blocking context, run `thunk` directly
- otherwise submit `thunk` to the blocking pool and wait for the result

This is better than the old inline implementation, but it is still not as strong as Cats Effect's fiber-aware
implementation because our runloop is synchronous.

If a program is currently running in the async context and hits `ParIO.blocking(thunk)`, the context handoff looks like this:

| Runtime context timeline |
| --- |
| `Async`: `unsafeRunLoop` is evaluating the current node |
| `Async`: `current = Blocking(thunk)` |
| `Async`: `runBlocking` submits `thunk` and waits for its future |
| `Blocking`: the submitted thunk runs on a blocking worker |
| `Async`: the original runloop resumes with `Pure(result)` |

If the program started with `unsafeRunSync`, replace `Async` in the first and last rows with `External`.

### `Parallel.par`

`Parallel.par` now uses the dedicated parallel pool and waits with an `ExecutorCompletionService`.

The important improvement is:

- it fails fast when one task completes with an error
- it cancels the rest

### `Sleep`

`Sleep` now uses the timer executor plus the blocking context.

This is better structured than raw `Thread.sleep`, but it is still not a truly asynchronous sleep. The runloop
still waits synchronously for the wakeup.

The handoff is:

| Runtime context timeline |
| --- |
| original context reaches `Sleep(duration)` |
| original context schedules a wakeup on the timer executor |
| blocking context waits for the wakeup signal |
| timer executor completes the signal |
| original context resumes with `Pure(())` |

## Current limitations

These are the main remaining weaknesses:

1. There is no async callback node in `ParIO`.
2. Because of that, `blocking` and `sleep` are still implemented with synchronous waiting in the runloop.
3. Cancellation is still best-effort and thread-interruption-based.
4. This is not a work-stealing fiber scheduler.

That means `ParIO` is getting cleaner and more honest, but it is still not trying to be Cats Effect.

## Why `ParIORuntime` is the right place for pool config

We do **not** want to turn `ParIO` itself into a class.

`ParIO` should stay:

- immutable
- pure
- just the program data

The runtime is where we want configurability.

That is why [ParIOApp.scala](/Users/dmgcodevil/dev/parapet/parapet-pario/src/main/scala/io/parapet/ParIOApp.scala) now uses a
`ParIORuntime`, while the core scheduler still only sees `Effect[F]` and `Parallel[F]`.

## Sources worth studying

### 1. Cats Effect Thread Model

Source:
- [Cats Effect Thread Model](https://typelevel.org/cats-effect/docs/thread-model)

Why it matters:
- explains the compute-vs-blocking split
- explains why blocking on compute threads is dangerous
- gives a very good mental model of an IO runloop and fibers

### 2. Cats Effect Schedulers

Source:
- [Cats Effect Schedulers](https://typelevel.org/cats-effect/docs/schedulers)

Why it matters:
- explains why a runtime wants separate scheduling strategies
- explains the work-stealing pool vs blocking pool split
- gives performance reasoning, not just API reasoning

### 3. Cats Effect Runtime Configuration

Source:
- [Cats Effect IORuntime Configuration](https://typelevel.org/cats-effect/docs/core/io-runtime-config)

Why it matters:
- shows what mature runtime configurability looks like
- useful reference for what we may eventually want in `ParIORuntimeConfig`

### 4. JDK `ThreadPoolExecutor`

Source:
- [JDK `ThreadPoolExecutor` docs](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/concurrent/ThreadPoolExecutor.html)

Why it matters:
- explains fixed pools, elastic behavior, queues, rejection, and keep-alive
- if we configure our own executors, this is the authoritative JVM reference

### 5. `scala.util.control.TailCalls`

Source:
- [Scala `TailCalls` docs](https://scala-lang.org/api/2.13.9/scala/util/control/TailCalls%24.html)

Why it matters:
- it is a standard-library trampoline reference
- it explicitly points back to the free-monad trampoline paper

### 6. "Stackless Scala with Free Monads"

Source:
- [Runar Bjarnason, *Stackless Scala with Free Monads*](https://blog.higher-order.com/assets/trampolines.pdf)

Why it matters:
- this is the conceptual ancestor of the `current` + continuation-stack style runloop
- especially relevant for understanding why we interpret with a loop and heap-allocated frames rather than ordinary
  nested method calls

## Practical reading order

If I were reviewing `ParIO` line by line, I would read in this order:

1. [ParIO.scala](/Users/dmgcodevil/dev/parapet/parapet-pario/src/main/scala/io/parapet/effect/ParIO.scala)
2. [ParIORuntime.scala](/Users/dmgcodevil/dev/parapet/parapet-pario/src/main/scala/io/parapet/effect/ParIORuntime.scala)
3. [Effect.scala](/Users/dmgcodevil/dev/parapet/parapet-core/src/main/scala/io/parapet/effect/Effect.scala)
4. [Cats Effect Thread Model](https://typelevel.org/cats-effect/docs/thread-model)
5. [Cats Effect Schedulers](https://typelevel.org/cats-effect/docs/schedulers)
6. [Stackless Scala with Free Monads](https://blog.higher-order.com/assets/trampolines.pdf)

That order gives:

- the code as it exists
- the abstraction boundary
- a strong runtime reference
- the stack-safety/trampoline theory behind the runloop
