package io.parapet.core

import io.parapet.core.annotations.developerApi
import io.parapet.free.{Free, Inject}
import io.parapet.{Event, ProcessRef}

import scala.concurrent.duration.FiniteDuration

object Dsl:

  type Dsl[F[_], A] = FlowOp[F, A]
  type DslF[F[_], A] = Free[[x] =>> Dsl[F, x], A]

  sealed trait FlowOp[F[_], A]

  final case class UnitFlow[F[_]]() extends FlowOp[F, Unit]
  final case class Pure[F[_], A](value: A) extends FlowOp[F, A]
  final case class Send[F[_]](
      event: () => Event,
      sender: Option[ProcessRef],
      receiver: ProcessRef,
      receivers: Seq[ProcessRef]
  ) extends FlowOp[F, Unit]
  final case class Forward[F[_]](event: () => Event, receivers: Seq[ProcessRef]) extends FlowOp[F, Unit]
  final case class Par[F[_], G[_]](flow: Free[G, Unit]) extends FlowOp[F, Unit]
  final case class Delay[F[_]](duration: FiniteDuration) extends FlowOp[F, Unit]
  final case class WithSender[F[_], G[_], A](f: ProcessRef => Free[G, A]) extends FlowOp[F, A]
  final case class Fork[F[_], G[_], A](flow: Free[G, A]) extends FlowOp[F, Fiber[F, A]]
  final case class Register[F[_]](parent: ProcessRef, child: Process[F]) extends FlowOp[F, Unit]
  final case class Race[F[_], G[_], A, B](first: Free[G, A], second: Free[G, B])
      extends FlowOp[F, Either[A, B]]
  final case class Suspend[F[_], G[_], A](thunk: () => F[A]) extends FlowOp[F, A]
  final case class SuspendF[F[_], G[_], A](thunk: () => Free[G, A]) extends FlowOp[F, A]
  final case class Eval[F[_], G[_], A](thunk: () => A) extends FlowOp[F, A]
  final case class Blocking[F[_], G[_], A](body: () => Free[G, A]) extends FlowOp[F, Unit]
  final case class RaiseError[F[_], A](error: Throwable) extends FlowOp[F, A]
  final case class HandleError[F[_], G[_], A, B >: A](body: () => Free[G, A], handle: Throwable => Free[G, B])
      extends FlowOp[F, B]
  final case class Halt[F[_]](ref: ProcessRef) extends FlowOp[F, Unit]
  final case class Guarantee[F[_], G[_], A](fa: () => Free[G, A], finalizer: () => Free[G, Unit])
      extends FlowOp[F, Unit]
  final case class Lock[F[_]](ref: ProcessRef) extends FlowOp[F, Unit]
  final case class Unlock[F[_]](ref: ProcessRef) extends FlowOp[F, Unit]

  class FlowOps[F[_], C[_]](using inject: Inject[[x] =>> FlowOp[F, x], C]):

    val unit: Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](UnitFlow[F]())

    def pure[A](value: A): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](Pure[F, A](value))

    def flow[A](value: => Free[C, A]): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](SuspendF[F, C, A](() => value))

    def send(event: => Event, receiver: ProcessRef, other: ProcessRef*): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Send[F](() => event, None, receiver, other))

    @developerApi
    def send(sender: ProcessRef, event: => Event, receiver: ProcessRef, other: ProcessRef*): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Send[F](() => event, Some(sender), receiver, other))

    def forward(event: => Event, receiver: ProcessRef, other: ProcessRef*): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Forward[F](() => event, receiver +: other))

    def par[A](flows: Free[C, A]*): Free[C, List[Fiber[F, A]]] =
      sequence(flows.toList.map(fork))

    def delay(duration: FiniteDuration): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Delay[F](duration))

    def withSender[A](f: ProcessRef => Free[C, A]): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](WithSender[F, C, A](f))

    def fork[A](flow: Free[C, A]): Free[C, Fiber[F, A]] =
      Free.inject[[x] =>> FlowOp[F, x], C, Fiber[F, A]](Fork[F, C, A](flow))

    def register(parent: ProcessRef, child: Process[F]*): Free[C, Unit] =
      child
        .map(process => Free.inject[[x] =>> FlowOp[F, x], C, Unit](Register(parent, process)))
        .foldLeft(unit) { (result, next) =>
          result.flatMap(_ => next)
        }

    def race[A, B](first: Free[C, A], second: Free[C, B]): Free[C, Either[A, B]] =
      Free.inject[[x] =>> FlowOp[F, x], C, Either[A, B]](Race[F, C, A, B](first, second))

    def suspend[A](thunk: => F[A]): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](Suspend[F, C, A](() => thunk))

    def eval[A](thunk: => A): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](Eval[F, C, A](() => thunk))

    def blocking[A](thunk: => Free[C, A]): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Blocking[F, C, A](() => thunk))

    def raiseError[A](error: Throwable): Free[C, A] =
      Free.inject[[x] =>> FlowOp[F, x], C, A](RaiseError[F, A](error))

    def handleError[A, B >: A](thunk: => Free[C, A], handle: Throwable => Free[C, B]): Free[C, B] =
      Free.inject[[x] =>> FlowOp[F, x], C, B](HandleError[F, C, A, B](() => thunk, handle))

    def guarantee[A](fa: => Free[C, A], finalizer: => Free[C, Unit]): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Guarantee[F, C, A](() => fa, () => finalizer))

    def halt(ref: ProcessRef): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Halt[F](ref))

    def lock(ref: ProcessRef): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Lock[F](ref))

    def unlock(ref: ProcessRef): Free[C, Unit] =
      Free.inject[[x] =>> FlowOp[F, x], C, Unit](Unlock[F](ref))

    private def sequence[A](values: List[Free[C, A]]): Free[C, List[A]] =
      values.foldRight(pure(List.empty[A])) { (current, acc) =>
        current.flatMap(value => acc.map(value :: _))
      }

  object FlowOps:
    type Aux[F[_]] = FlowOps[F, [x] =>> Dsl[F, x]]

    given [F[_], G[_]](using Inject[[x] =>> FlowOp[F, x], G]): FlowOps[F, G] =
      new FlowOps[F, G]

  trait WithDsl[F[_]]:
    protected val dsl: FlowOps[F, [x] =>> Dsl[F, x]] =
      summon[FlowOps[F, [x] =>> Dsl[F, x]]]
