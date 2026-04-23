package io.parapet.core

import io.parapet.core.annotations.developerApi
import io.parapet.free.{Free, Inject}
import io.parapet.{Event, ProcessRef}

import scala.concurrent.duration.FiniteDuration

/** The parapet process DSL: an algebra of effectful operations a [[Process]] can perform.
  *
  * Programs are built as values of [[Dsl.DslF]] using the operators on [[Dsl.FlowOps]] (sending
  * events, forking fibers, racing concurrent computations, etc.) and later interpreted by
  * the runtime via [[DslInterpreter]] into the user's effect type `F[_]`.
  *
  * Encoding the DSL as a free monad over [[Dsl.FlowOp]] means programs are just data —
  * inspectable, composable, and decoupled from any particular effect runtime. The same
  * program runs on any effect type that provides an [[io.parapet.effect.Effect]] instance
  * (parapet's own [[io.parapet.effect.ParIO]] is the bundled implementation).
  */
object Dsl:

  /** Alias for the underlying algebra. The `Dsl` parameter is normally what user code
    * speaks in.
    */
  type Dsl[F[_], A] = FlowOp[F, A]

  /** A program in the parapet DSL: a [[Free]] computation over [[FlowOp]] producing `A`.
    * Most user-facing methods on [[Process]] use the alias `Process#Program = DslF[F, Unit]`.
    */
  type DslF[F[_], A] = Free[[x] =>> Dsl[F, x], A]

  /** Root of the algebra. Each `FlowOp` is an instruction the [[DslInterpreter]] knows how
    * to run; user code does not normally pattern-match on these directly.
    */
  sealed trait FlowOp[F[_], A]

  /** No-op instruction that produces unit. */
  final case class UnitFlow[F[_]]() extends FlowOp[F, Unit]

  /** Lifts a pure value `A` into the program. */
  final case class Pure[F[_], A](value: A) extends FlowOp[F, A]

  /** Sends `event` to `receiver` (and any additional `receivers`).
    *
    * The thunk is evaluated lazily so the runtime can elide construction when delivery is
    * not possible (e.g. unknown receiver). When `sender` is empty the runtime supplies the
    * current process's ref; explicit values are reserved for advanced re-sender scenarios.
    */
  final case class Send[F[_]](
      event: () => Event,
      sender: Option[ProcessRef],
      receiver: ProcessRef,
      receivers: Seq[ProcessRef]
  ) extends FlowOp[F, Unit]

  /** Re-emits `event` to `receivers` while preserving the original sender of the event
    * currently being handled.
    */
  final case class Forward[F[_]](event: () => Event, receivers: Seq[ProcessRef]) extends FlowOp[F, Unit]

  /** Runs `flow` concurrently and waits for it to finish. */
  final case class Par[F[_], G[_]](flow: Free[G, Unit]) extends FlowOp[F, Unit]

  /** Suspends the current program for `duration` without blocking the underlying thread. */
  final case class Delay[F[_]](duration: FiniteDuration) extends FlowOp[F, Unit]

  /** Provides the current sender's [[ProcessRef]] to the body. Useful for replying inside
    * a handler.
    */
  final case class WithSender[F[_], G[_], A](f: ProcessRef => Free[G, A]) extends FlowOp[F, A]

  /** Forks `flow` as a [[Fiber]] that runs concurrently; the fiber handle is returned for
    * later join/cancel.
    */
  final case class Fork[F[_], G[_], A](flow: Free[G, A]) extends FlowOp[F, Fiber[F, A]]

  /** Registers `child` as a sub-process of `parent`, integrating it into the supervision
    * graph.
    */
  final case class Register[F[_]](parent: ProcessRef, child: Process[F]) extends FlowOp[F, Unit]

  /** Races `first` against `second` and returns whichever wins, cancelling the loser. */
  final case class Race[F[_], G[_], A, B](first: Free[G, A], second: Free[G, B])
      extends FlowOp[F, Either[A, B]]

  /** Suspends a side-effecting computation in the underlying effect type `F`. */
  final case class Suspend[F[_], G[_], A](thunk: () => F[A]) extends FlowOp[F, A]

  /** Defers construction of a [[DslF]] sub-program until execution time. */
  final case class SuspendF[F[_], G[_], A](thunk: () => Free[G, A]) extends FlowOp[F, A]

  /** Evaluates a pure but lazy `A`. */
  final case class Eval[F[_], G[_], A](thunk: () => A) extends FlowOp[F, A]

  /** Marks `body` as blocking; the runtime offloads it so it does not stall worker threads. */
  final case class Blocking[F[_], G[_], A](body: () => Free[G, A]) extends FlowOp[F, Unit]

  /** Aborts the program with `error`. */
  final case class RaiseError[F[_], A](error: Throwable) extends FlowOp[F, A]

  /** Runs `body`; if it raises, defers to `handle` to recover. */
  final case class HandleError[F[_], G[_], A, B >: A](body: () => Free[G, A], handle: Throwable => Free[G, B])
      extends FlowOp[F, B]

  /** Stops the process identified by `ref`, cascading to its descendants. */
  final case class Halt[F[_]](ref: ProcessRef) extends FlowOp[F, Unit]

  /** Runs `fa` and guarantees `finalizer` runs afterwards regardless of success or failure. */
  final case class Guarantee[F[_], G[_], A](fa: () => Free[G, A], finalizer: () => Free[G, Unit])
      extends FlowOp[F, Unit]

  /** Acquires the per-process lock for `ref` (cooperative mutual exclusion). */
  final case class Lock[F[_]](ref: ProcessRef) extends FlowOp[F, Unit]

  /** Releases a previously acquired [[Lock]]. */
  final case class Unlock[F[_]](ref: ProcessRef) extends FlowOp[F, Unit]

  /** Smart constructors for the [[FlowOp]] algebra.
    *
    * `FlowOps` is the user-facing surface of the DSL. It is normally summoned implicitly
    * via the `dsl` accessor on [[Process]] (see [[Dsl.WithDsl]]).
    *
    * @tparam F the underlying effect type.
    * @tparam C the algebra coproduct in which `FlowOp` sits — usually just
    *           `[x] =>> FlowOp[F, x]`, but generalized so users can mix the parapet DSL
    *           with their own algebras.
    */
  class FlowOps[F[_], C[_]](using inject: Inject[[x] =>> FlowOp[F, x], C]):

    /** A program that does nothing. */
    val unit: Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](UnitFlow[F]())

    /** Lifts a pure value into the program. */
    def pure[A](value: A): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](Pure[F, A](value))

    /** Defers construction of `value` until interpretation; equivalent to wrapping a
      * sub-program in a thunk.
      */
    def flow[A](value: => Free[C, A]): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](SuspendF[F, C, A](() => value))

    /** Sends `event` to `receiver` (and any additional refs in `other`). The current
      * process is used as the sender.
      */
    def send(event: => Event, receiver: ProcessRef, other: ProcessRef*): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Send[F](() => event, None, receiver, other))

    /** Sends `event` impersonating `sender`. Reserved for runtime-internal re-routing.
      *
      * Misuse breaks sender attribution and is rejected by [[io.parapet.core.processes.SystemProcess]].
      */
    @developerApi
    def send(sender: ProcessRef, event: => Event, receiver: ProcessRef, other: ProcessRef*): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Send[F](() => event, Some(sender), receiver, other))

    /** Forwards `event` to `receiver` (and `other`) preserving the original sender. */
    def forward(event: => Event, receiver: ProcessRef, other: ProcessRef*): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Forward[F](() => event, receiver +: other))

    /** Forks every program in `flows` concurrently and returns their fibers. */
    def par[A](flows: Free[C, A]*): Free[C, List[Fiber[F, A]]] =
      sequence(flows.toList.map(fork))

    /** Non-blocking sleep — suspends the current program for `duration`. */
    def delay(duration: FiniteDuration): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Delay[F](duration))

    /** Runs `f` with the current sender's [[ProcessRef]] in scope. The most idiomatic way to
      * reply to the originator of the event being handled.
      */
    def withSender[A](f: ProcessRef => Free[C, A]): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](WithSender[F, C, A](f))

    /** Forks `flow` as a [[Fiber]]. The fiber runs concurrently and can be awaited or
      * cancelled via the returned handle.
      */
    def fork[A](flow: Free[C, A]): Free[C, Fiber[F, A]] =
      Free.inject[[x] =>> FlowOp[F, x], C, Fiber[F, A]](Fork[F, C, A](flow))

    /** Registers each process in `child` as a sub-process of `parent`. */
    def register(parent: ProcessRef, child: Process[F]*): Free[C, Unit] =
      child
        .map(process => Free.inject[[x] =>> FlowOp[F, x], C, Unit](Register(parent, process)))
        .foldLeft(unit) { (result, next) =>
          result.flatMap(_ => next)
        }

    /** Races `first` against `second`. The loser is cancelled. */
    def race[A, B](first: Free[C, A], second: Free[C, B]): Free[C, Either[A, B]] =
      Free.inject[[x] =>> FlowOp[F, x], C, Either[A, B]](Race[F, C, A, B](first, second))

    /** Lifts a side-effecting `F`-program into the DSL. */
    def suspend[A](thunk: => F[A]): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](Suspend[F, C, A](() => thunk))

    /** Evaluates a pure but lazy `A`. */
    def eval[A](thunk: => A): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](Eval[F, C, A](() => thunk))

    /** Marks `thunk` as blocking; the runtime offloads it so it does not stall scheduler
      * worker threads.
      */
    def blocking[A](thunk: => Free[C, A]): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Blocking[F, C, A](() => thunk))

    /** Aborts the program with `error`; recoverable via [[handleError]]. */
    def raiseError[A](error: Throwable): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](RaiseError[F, A](error))

    /** Runs `thunk`; on failure delegates to `handle` to recover. Mirrors `try / catch`. */
    def handleError[A, B >: A](thunk: => Free[C, A], handle: Throwable => Free[C, B]): Free[C, B] =
      Free.inject[[x] =>> FlowOp[F, x], C, B](HandleError[F, C, A, B](() => thunk, handle))

    /** Runs `fa` and ensures `finalizer` runs afterwards regardless of outcome. Mirrors
      * `try / finally`.
      */
    def guarantee[A](fa: => Free[C, A], finalizer: => Free[C, Unit]): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Guarantee[F, C, A](() => fa, () => finalizer))

    /** Stops the process identified by `ref` and any descendants. */
    def halt(ref: ProcessRef): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Halt[F](ref))

    /** Acquires the per-process lock for `ref`. Cooperative — only meaningful between
      * programs that opt into it.
      */
    def lock(ref: ProcessRef): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Lock[F](ref))

    /** Releases the per-process lock for `ref`. */
    def unlock(ref: ProcessRef): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Unlock[F](ref))

    private def sequence[A](values: List[Free[C, A]]): Free[C, List[A]] =
      values.foldRight(pure(List.empty[A])) { (current, acc) =>
        current.flatMap(value => acc.map(value :: _))
      }

  /** Implicit-priority machinery for summoning a [[FlowOps]] instance for arbitrary
    * algebra coproducts.
    */
  object FlowOps:
    /** The most common shape: `FlowOps` over the parapet DSL alone. */
    type Aux[F[_]] = FlowOps[F, [x] =>> Dsl[F, x]]

    /** Auto-derives a `FlowOps` instance whenever there is an [[Inject]] from the parapet
      * algebra into a coproduct `G`.
      */
    given [F[_], G[_]](using Inject[[x] =>> FlowOp[F, x], G]): FlowOps[F, G] =
      new FlowOps[F, G]

  /** Mixin granting access to the `dsl` member — a [[FlowOps]] instance over the parapet
    * algebra. Mixed into [[Process]] so that handlers can write `dsl.send(...)`,
    * `dsl.eval(...)`, etc.
    */
  trait WithDsl[F[_]]:
    protected val dsl: FlowOps[F, [x] =>> Dsl[F, x]] =
      summon[FlowOps[F, [x] =>> Dsl[F, x]]]
