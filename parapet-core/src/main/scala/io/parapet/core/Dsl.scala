package io.parapet.core

import io.parapet.core.annotations.developerApi
import io.parapet.free.{Free, Inject}
import io.parapet.{Event, ProcessRef, Scope}

import scala.concurrent.duration.FiniteDuration

/** The parapet process DSL: an algebra of effectful operations a [[Process]] can perform.
  *
  * Programs are built as values of [[Dsl.DslF]] using the operators on [[Dsl.FlowOps]] (sending events, forking fibers,
  * racing concurrent computations, etc.) and later interpreted by the runtime via [[DslInterpreter]] into the user's
  * effect type `F[_]`.
  *
  * Encoding the DSL as a free monad over [[Dsl.FlowOp]] means programs are just data - inspectable, composable, and
  * decoupled from any particular effect runtime. The same program runs on any effect type that provides the core
  * capability instances, such as a Cats Effect backend or a custom runtime.
  */
object Dsl:

  /** Alias for the underlying algebra. The `Dsl` parameter is normally what user code speaks in.
    */
  type Dsl[F[_], A] = FlowOp[F, A]

  /** A program in the parapet DSL: a [[Free]] computation over [[FlowOp]] producing `A`. Most user-facing methods on
    * [[Process]] use the alias `Process#Program = DslF[F, Unit]`.
    */
  type DslF[F[_], A] = Free[[x] =>> Dsl[F, x], A]

  /** Root of the algebra. Each `FlowOp` is an instruction the [[DslInterpreter]] knows how to run; user code does not
    * normally pattern-match on these directly.
    */
  sealed trait FlowOp[F[_], A]

  /** No-op instruction that produces unit. */
  final case class UnitFlow[F[_]]() extends FlowOp[F, Unit]

  /** Lifts a pure value `A` into the program. */
  final case class Pure[F[_], A](value: A) extends FlowOp[F, A]

  /** Sends `event` to `receiver` (and any additional `receivers`).
    *
    * The thunk is evaluated lazily so the runtime can elide construction when delivery is not possible (e.g. unknown
    * receiver). When `sender` is empty the runtime supplies the current process's ref; explicit values are reserved for
    * advanced re-sender scenarios.
    */
  final case class Send[F[_]](
      event: () => Event,
      sender: Option[ProcessRef.Unknown],
      receiver: ProcessRef.Unknown,
      receivers: Seq[ProcessRef.Unknown]
  ) extends FlowOp[F, Unit]

  /** Re-emits `event` to `receivers` while preserving the original sender of the event currently being handled.
    */
  final case class Forward[F[_]](event: () => Event, receivers: Seq[ProcessRef.Unknown]) extends FlowOp[F, Unit]

  /** Runs `flow` concurrently and waits for it to finish. */
  final case class Par[F[_], G[_]](flow: Free[G, Unit]) extends FlowOp[F, Unit]

  /** Suspends the current program for `duration` without blocking the underlying thread. */
  final case class Delay[F[_]](duration: FiniteDuration) extends FlowOp[F, Unit]

  /** Provides the current sender's [[ProcessRef]] to the body. Useful for replying inside a handler.
    */
  final case class WithSender[F[_], G[_], A](f: ProcessRef[Event] => Free[G, A]) extends FlowOp[F, A]

  /** Forks `flow` as a [[Fiber]] that runs concurrently; the fiber handle is returned for later join/cancel.
    */
  final case class Fork[F[_], G[_], A](flow: Free[G, A]) extends FlowOp[F, Fiber[F, A]]

  /** Registers `child` as a sub-process of `parent`, integrating it into the supervision graph.
    */
  final case class Register[F[_]](parent: ProcessRef.Unknown, child: Process[F, ?, ?]) extends FlowOp[F, Unit]

  /** Races `first` against `second` and returns whichever wins, cancelling the loser. */
  final case class Race[F[_], G[_], A, B](first: Free[G, A], second: Free[G, B]) extends FlowOp[F, Either[A, B]]

  /** Suspends a side-effecting computation in the underlying effect type `F`. */
  final case class Suspend[F[_], G[_], A](thunk: () => F[A]) extends FlowOp[F, A]

  /** Defers construction of a [[DslF]] sub-program until execution time. */
  final case class SuspendF[F[_], G[_], A](thunk: () => Free[G, A]) extends FlowOp[F, A]

  /** Evaluates a pure but lazy `A`. */
  final case class Eval[F[_], G[_], A](thunk: () => A) extends FlowOp[F, A]

  /** Starts `body` on a background effect fiber while keeping the current handler flow running.
    *
    * Semantics:
    *   - `body` is started on a background effect fiber
    *   - the current handler flow continues immediately
    *   - the process does not accept its next mailbox event until all offloaded work completes
    */
  final case class Offload[F[_], G[_], A](body: () => Free[G, A]) extends FlowOp[F, Unit]

  /** Aborts the program with `error`. */
  final case class RaiseError[F[_], A](error: Throwable) extends FlowOp[F, A]

  /** Runs `body`; if it raises, defers to `handle` to recover. */
  final case class HandleError[F[_], G[_], A, B >: A](body: () => Free[G, A], handle: Throwable => Free[G, B])
      extends FlowOp[F, B]

  /** Stops the process identified by `ref`, cascading to its descendants. */
  final case class Halt[F[_]](ref: ProcessRef.Unknown) extends FlowOp[F, Unit]

  /** Runs `fa` and guarantees `finalizer` runs afterwards regardless of success or failure. */
  final case class Guarantee[F[_], G[_], A](fa: () => Free[G, A], finalizer: () => Free[G, Unit])
      extends FlowOp[F, Unit]

  /** Acquires the per-process lock for `ref` (cooperative mutual exclusion). */
  final case class Lock[F[_]](ref: ProcessRef.Unknown) extends FlowOp[F, Unit]

  /** Releases a previously acquired [[Lock]]. */
  final case class Unlock[F[_]](ref: ProcessRef.Unknown) extends FlowOp[F, Unit]

  /** Reads the interpreter's current [[io.parapet.Scope]].
    *
    * The current scope is reader-context metadata, normally copied from the incoming [[io.parapet.Envelope]]. Reading
    * it does not mutate anything. The continuation `f` builds the next program step after seeing the scope, and that
    * next step continues under the same scope.
    */
  final case class WithScope[F[_], G[_], A](f: Scope => Free[G, A]) extends FlowOp[F, A]

  /** Runs `body` under a transformed scope.
    *
    * The interpreter evaluates `f(currentScope)` once when entering the block, then interprets `body` with that derived
    * scope. Every outbound envelope emitted by `body` carries the derived scope. When `body` completes, the outer scope
    * is restored for the rest of the program, so this is a lexical override rather than a mutation of global/runtime
    * state.
    */
  final case class MapScope[F[_], G[_], A](f: Scope => Scope, body: Free[G, A]) extends FlowOp[F, A]

  /** Smart constructors for the [[FlowOp]] algebra.
    *
    * `FlowOps` is the user-facing surface of the DSL. It is normally summoned implicitly via the `dsl` accessor on
    * [[Process]] (see [[Dsl.WithDsl]]).
    *
    * @tparam F
    *   the underlying effect type.
    * @tparam C
    *   the algebra coproduct in which `FlowOp` sits - usually just `[x] =>> FlowOp[F, x]`, but generalized so users can
    *   mix the parapet DSL with their own algebras.
    */
  class FlowOps[F[_], C[_]](using inject: Inject[[x] =>> FlowOp[F, x], C]):

    /** A program that does nothing - semantically equivalent to `Monad.unit`.
      *
      * Useful as a fold seed and as the explicit "do nothing" branch of a handler.
      *
      * The following expressions are equivalent:
      *
      * {{{
      * event ~> process <-> unit ++ event ~> process
      * event ~> process <-> event ~> process ++ unit
      * }}}
      *
      * Example - combining flows over a list:
      *
      * {{{
      * processes.map(event ~> _).foldLeft(unit)(_ ++ _)
      * }}}
      *
      * Example - empty handler branches:
      *
      * {{{
      * {
      *   case Start => unit // do nothing
      *   case Stop  => unit // do nothing
      * }
      * }}}
      */
    val unit: Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](UnitFlow[F]())

    /** Lifts a pure value into the program.
      *
      * Example:
      *
      * {{{
      * for
      *   a <- pure(1)
      *   _ <- eval(println(a))
      * yield ()
      * }}}
      */
    def pure[A](value: A): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](Pure[F, A](value))

    /** Defers construction of a sub-program until interpretation.
      *
      * The main use is writing recursive flows - without `flow`, recursion happens at program-construction time and
      * overflows the stack before the runtime ever sees the program. With `flow`, each recursive step is built on
      * demand.
      *
      * Example - prints `n n-1 ... 1`:
      *
      * {{{
      * def times[F[_]](n: Int): DslF[F, Unit] = flow {
      *   if n == 0 then unit
      *   else eval(print(n)) ++ times(n - 1)
      * }
      * }}}
      *
      * Note: do not perform side effects directly inside `flow` - the body runs at interpretation time but only to
      * build the next step of the program, so anything outside [[eval]] / [[suspend]] is still effectively
      * construction-time code.
      */
    def flow[A](value: => Free[C, A]): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](SuspendF[F, C, A](() => value))

    /** Lazily constructs and sends `event` to `receiver` (and any additional refs in `other`). The current process is
      * recorded as the sender.
      *
      * Events are emitted in the order given. Delivery order across receivers is not guaranteed (it depends on each
      * receiver's mailbox depth and processing speed).
      *
      * Example:
      *
      * {{{
      * send(Ping, processA, processB, processC)
      * }}}
      *
      * `Ping` is enqueued for `processA`, then `processB`, then `processC`. The symbolic `~>` form is preferred for the
      * single-receiver case:
      *
      * {{{
      * Ping ~> processA
      * Seq(e1, e2, e3) ~> processA  // batch in order
      * }}}
      */
    def send[E <: Event](
        event: => E,
        receiver: ProcessRef[? >: E],
        other: ProcessRef[? >: E]*
    ): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Send[F](() => event, None, receiver, other))

    /** Sends `event` impersonating `sender`. Reserved for runtime-internal re-routing.
      *
      * Misuse breaks sender attribution and is rejected by [[io.parapet.core.processes.SystemProcess]].
      */
    @developerApi
    def send[E <: Event](
        sender: ProcessRef.Unknown,
        event: => E,
        receiver: ProcessRef[? >: E],
        other: ProcessRef[? >: E]*
    ): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Send[F](() => event, Some(sender), receiver, other))

    /** Forwards `event` to `receiver` (and `other`) while preserving the sender of the event currently being handled -
      * the typical building block of a proxy process.
      *
      * Use this for proxy-style processes that should preserve the original sender for a downstream [[Process.reply]].
      */
    def forward[E <: Event](
        event: => E,
        receiver: ProcessRef[? >: E],
        other: ProcessRef[? >: E]*
    ): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Forward[F](() => event, receiver +: other))

    /** Runs the given `flows` in parallel and returns their [[Fiber]] handles for later `join` / `cancel`.
      *
      * Example:
      *
      * {{{
      * par(eval(print(1)) ++ eval(print(2)))
      * }}}
      *
      * Possible outputs: `12` or `21`.
      */
    def par[A](flows: Free[C, A]*): Free[C, List[Fiber[F, A]]] =
      sequence(flows.toList.map(fork))

    /** Suspends every operation that follows for `duration`.
      *
      * Runtime implementations may block a runtime thread unless they support true async suspension.
      *
      * For sequential flows the following are equivalent:
      *
      * {{{
      * delay(d) ++ x ~> p ++ y ~> p
      * delay(d, x ~> p ++ y ~> p)
      * delay(d, x ~> p) ++ delay(d, y ~> p)
      * }}}
      *
      * For parallel flows the delay applies once before the parallel block begins:
      *
      * {{{
      * delay(d, par(x ~> p ++ y ~> p)) <-> delay(d) ++ par(x ~> p ++ y ~> p)
      * }}}
      *
      * Note: inside `par` only the first operation is delayed; wrap each parallel branch to delay every one of them
      * individually:
      *
      * {{{
      * par(delay(d, eval(print(1))), delay(d, eval(print(2))))
      * }}}
      */
    def delay(duration: FiniteDuration): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Delay[F](duration))

    /** Low-level untyped reply helper for core internals.
      *
      * User processes should call [[Process.reply]], which checks the event against the process's declared `Out`
      * protocol. This helper stays untyped because the generic DSL does not know which process is currently building a
      * program.
      */
    @developerApi
    private[core] def reply(event: => Event): Free[C, Unit] =
      unsafe.withSender(sender => send(event, sender))

    /** Low-level untyped batch reply helper for core internals. */
    @developerApi
    private[core] def reply(events: Seq[Event]): Free[C, Unit] =
      unsafe.withSender { sender =>
        events.toList match
          case Nil          => unit
          case head :: tail =>
            tail.foldLeft(send(head, sender)) { (acc, event) =>
              acc.flatMap(_ => send(event, sender))
            }
      }

    /** Reads the current [[Scope]] and builds the next step from it. */
    @developerApi
    private[core] def withScope[A](f: Scope => Free[C, A]): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](WithScope[F, C, A](f))

    /** Runs `body` under the scope produced by `f(currentScope)`.
      *
      * The transformation is lexical: outbound envelopes created inside `body` carry the derived scope, but once `body`
      * finishes (whether by success or by raising), subsequent operations continue with the outer scope.
      */
    @developerApi
    private[core] def mapScope[A](f: Scope => Scope)(body: Free[C, A]): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](MapScope[F, C, A](f, body))

    /** Executes `flow` asynchronously as a [[Fiber]]. The fiber runs concurrently and the returned handle can be
      * awaited via [[Fiber.join]] or cancelled.
      *
      * Example:
      *
      * {{{
      * val process = Process[F](_ => {
      *   case Start => fork(eval(print(1))) ++ fork(eval(print(2)))
      * })
      * }}}
      *
      * Possible outputs: `12` or `21`.
      *
      * Awaiting a fiber's result:
      *
      * {{{
      * for
      *   fiber <- fork(eval("long running operation"))
      *   res   <- fiber.join
      *   _     <- eval(print(res))
      * yield ()
      * }}}
      */
    def fork[A](flow: Free[C, A]): Free[C, Fiber[F, A]] =
      Free.inject[[x] =>> FlowOp[F, x], C, Fiber[F, A]](Fork[F, C, A](flow))

    /** Registers one or more child processes under `parent`.
      *
      * Children are guaranteed to receive [[io.parapet.core.Events.Stop]] before their parent does - useful for
      * releasing resources in the right order.
      *
      * Example:
      *
      * {{{
      * val server = Process[F](ref => {
      *   case Start =>
      *     register(ref, Process[F](_ => {
      *       case Stop => eval(println("stop worker"))
      *     }))
      *   case Stop  => eval(println("stop server"))
      * })
      * }}}
      *
      * Console output on shutdown:
      *
      * {{{
      * stop worker
      * stop server
      * }}}
      */
    def register(parent: ProcessRef.Unknown, child: Process[F, ?, ?]*): Free[C, Unit] =
      child
        .map(process => Free.inject[[x] =>> FlowOp[F, x], C, Unit](Register(parent, process)))
        .foldLeft(unit) { (result, next) =>
          result.flatMap(_ => next)
        }

    /** Runs `first` and `second` concurrently and returns whichever wins. The loser is cancelled.
      *
      * Example:
      *
      * {{{
      * val forever = eval(while true do {})
      *
      * val process = Process[F](_ => {
      *   case Start => race(forever, eval(println("winner")))
      * })
      * }}}
      *
      * Output: `winner`.
      */
    def race[A, B](first: Free[C, A], second: Free[C, B]): Free[C, Either[A, B]] =
      Free.inject[[x] =>> FlowOp[F, x], C, Either[A, B]](Race[F, C, A, B](first, second))

    /** Suspends a side-effecting `F`-program inside the DSL.
      *
      * Example:
      *
      * {{{
      * suspend(effect.delay(println("hello world")))
      * }}}
      *
      * Prefer [[eval]] for plain (non-`F`) side effects; reserve `suspend` for cases where you genuinely need to lift
      * an existing `F[A]`.
      */
    def suspend[A](thunk: => F[A]): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](Suspend[F, C, A](() => thunk))

    /** Suspends a pure but lazy computation. The thunk is evaluated when the DSL program is interpreted, not when the
      * program value is constructed.
      *
      * Example:
      *
      * {{{
      * eval(println("hello world"))
      * }}}
      *
      * Output: `hello world`.
      */
    def eval[A](thunk: => A): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](Eval[F, C, A](() => thunk))

    /** Starts `thunk` on a background effect fiber so it does not stall scheduler worker threads.
      *
      * Example:
      *
      * {{{
      * class BlockingProcess extends Process[F]:
      *   override def handle: Receive =
      *     case Start => offload(eval(while true do {})) ++ eval(println("now"))
      * }}}
      *
      * Output: `now`. The infinite loop runs on a separate worker, so the current handler flow can continue
      * immediately.
      *
      * Important: `offload(thunk) ++ next` does **not** wait for `thunk` to finish before `next` runs. The barrier is
      * at the process boundary, not at the current DSL step:
      *
      *   - later steps in the same handler flow may run immediately
      *   - the process will not accept its next mailbox event until all offloaded work completes
      *
      * This differs from [[fork]] in one important way: a forked program is fully fire-and-forget, so the parent
      * process keeps consuming new mailbox events while it runs. `offload` still gates the process before the next
      * mailbox event.
      */
    def offload[A](thunk: => Free[C, A]): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Offload[F, C, A](() => thunk))

    /** Deprecated alias for [[offload]]. The old name suggested inline waiting semantics that are not actually
      * provided.
      */
    @deprecated(
      "Use offload(...) instead. blocking(...) starts work in the background and only gates the next mailbox event; it does not wait inline before the next ++ step.",
      since = "2026-05"
    )
    def blocking[A](thunk: => Free[C, A]): Free[C, Unit] =
      offload(thunk)

    /** Aborts the program with `error`. Subsequent steps are skipped unless the error is caught by [[handleError]].
      */
    def raiseError[A](error: Throwable): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](RaiseError[F, A](error))

    /** Runs `thunk`; on failure delegates to `handle` to recover. Mirrors `try / catch`.
      *
      * Example:
      *
      * {{{
      * handleError(
      *   eval(throw RuntimeException("boom")),
      *   err => eval(println(s"caught: ${err.getMessage}"))
      * )
      * }}}
      *
      * Console output: `caught: boom`.
      */
    def handleError[A, B >: A](thunk: => Free[C, A], handle: Throwable => Free[C, B]): Free[C, B] =
      Free.inject[[x] =>> FlowOp[F, x], C, B](HandleError[F, C, A, B](() => thunk, handle))

    /** Runs `fa` and ensures `finalizer` runs afterwards, regardless of success or failure. Mirrors `try / finally`.
      *
      * Example:
      *
      * {{{
      * guarantee(
      *   eval(throw RuntimeException("error")),
      *   eval(println("fallback"))
      * )
      * }}}
      *
      * Console output: `fallback` (followed by the rethrown error).
      */
    def guarantee[A](fa: => Free[C, A], finalizer: => Free[C, Unit]): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Guarantee[F, C, A](() => fa, () => finalizer))

    /** Stops the process identified by `ref` and any descendants. */
    def halt(ref: ProcessRef.Unknown): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Halt[F](ref))

    object unsafe {

      /** Runs `f` with the current sender's [[ProcessRef]] in scope.
        *
        * This is intentionally core-private because it exposes the raw, untyped sender ref. User code should use
        * [[Process.reply]], which checks replies against the process's declared `Out` protocol.
        */
      @developerApi
      def withSender[A](f: ProcessRef[Event] => Free[C, A]): Free[C, A] =
        Free.inject[[x] =>> FlowOp[F, x], C, A](WithSender[F, C, A](f))

    }

    private def sequence[A](values: List[Free[C, A]]): Free[C, List[A]] =
      values.foldRight(pure(List.empty[A])) { (current, acc) =>
        current.flatMap(value => acc.map(value :: _))
      }

  /** Implicit-priority machinery for summoning a [[FlowOps]] instance for arbitrary algebra coproducts.
    */
  object FlowOps:
    /** The most common shape: `FlowOps` over the parapet DSL alone. */
    type Aux[F[_]] = FlowOps[F, [x] =>> Dsl[F, x]]

    /** Auto-derives a `FlowOps` instance whenever there is an [[Inject]] from the parapet algebra into a coproduct `G`.
      */
    given [F[_], G[_]](using Inject[[x] =>> FlowOp[F, x], G]): FlowOps[F, G] =
      new FlowOps[F, G]

  /** Runtime-only helpers that manipulate scheduler/process machinery rather than expressing normal application
    * behavior.
    *
    * Unlike [[FlowOps]], this is not exposed through the default `dsl` member on [[Process]]. Callers must opt in
    * explicitly, which keeps process-lock operations out of the normal user-facing DSL surface.
    */
  private[core] class RuntimeOps[F[_], C[_]](using inject: Inject[[x] =>> FlowOp[F, x], C]):

    /** Acquires the runtime per-process delivery lock for `ref`.
      *
      * This is not a general-purpose application mutex. It is intended for runtime helpers that need to coordinate
      * directly with the scheduler's process-serialization machinery.
      */
    def lockProcess(ref: ProcessRef.Unknown): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Lock[F](ref))

    /** Releases the runtime per-process delivery lock for `ref`, and re-notifies the scheduler to resume draining that
      * process mailbox.
      */
    def unlockProcess(ref: ProcessRef.Unknown): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Unlock[F](ref))

  private[core] object RuntimeOps:
    type Aux[F[_]] = RuntimeOps[F, [x] =>> Dsl[F, x]]

    given [F[_], G[_]](using Inject[[x] =>> FlowOp[F, x], G]): RuntimeOps[F, G] =
      new RuntimeOps[F, G]

  /** Mixin granting access to the `dsl` member - a [[FlowOps]] instance over the parapet algebra. Mixed into
    * [[Process]] so that handlers can write `dsl.send(...)`, `dsl.eval(...)`, etc.
    */
  trait WithDsl[F[_]]:
    protected val dsl: FlowOps[F, [x] =>> Dsl[F, x]] =
      summon[FlowOps[F, [x] =>> Dsl[F, x]]]
