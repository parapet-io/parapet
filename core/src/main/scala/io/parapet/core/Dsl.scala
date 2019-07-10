package io.parapet.core

import cats.InjectK
import cats.data.EitherK
import cats.free.Free

import scala.concurrent.duration.FiniteDuration

object Dsl {

  type <:<[F[_], G[_], A] = EitherK[F, G, A]
  type FlowOpOrEffect[F[_], A] = <:<[FlowOp[F, ?], Effect[F, ?], A]
  type Dsl[F[_], A] = FlowOpOrEffect[F, A]
  type DslF[F[_], A] = Free[Dsl[F, ?], A]

  // <----------- Flow ADT ----------->
  sealed trait FlowOp[F[_], A]

  case class Empty[F[_]]() extends FlowOp[F, Unit]

  case class Use[F[_], G[_], A](resource: () => A, flow: A => Free[G, Unit]) extends FlowOp[F, Unit]

  case class Send[F[_]](e: Event, receivers: Seq[ProcessRef]) extends FlowOp[F, Unit]

  case class Forward[F[_]](e: Event, receivers: Seq[ProcessRef]) extends FlowOp[F, Unit]

  case class Par[F[_], G[_]](flow: Seq[Free[G, Unit]]) extends FlowOp[F, Unit]

  case class Delay[F[_], G[_]](duration: FiniteDuration, flow: Option[Free[G, Unit]]) extends FlowOp[F, Unit]

  case class Reply[F[_], G[_]](f: ProcessRef => Free[G, Unit]) extends FlowOp[F, Unit]

  case class Invoke[F[_], G[_]](caller: ProcessRef, body: Free[G, Unit], callee: ProcessRef) extends FlowOp[F, Unit]

  case class Fork[F[_], G[_]](f: Free[G, Unit]) extends FlowOp[F, Unit]

  case class Await[F[_], G[_]](
                                selector: PartialFunction[Event, Unit],
                                onTimeout: Free[G, Unit],
                                timeout: FiniteDuration
                              ) extends FlowOp[F, Unit]

  // F - effect type
  // C - coproduct of FlowOp and other algebras
  class FlowOps[F[_], C[_]](implicit I: InjectK[FlowOp[F, ?], C]) {
    val empty: Free[C, Unit] = Free.inject[FlowOp[F, ?], C](Empty())

    def flow(f: => Free[C, Unit]): Free[C, Unit] = use(())(_ => f)

    def use[A](resource: => A)(f: A => Free[C, Unit]): Free[C, Unit] = Free.inject[FlowOp[F, ?],C](Use(() => resource, f))

    // sends event `e` to the list of receivers
    def send(e: Event, receiver: ProcessRef, other: ProcessRef*): Free[C, Unit] = Free.inject[FlowOp[F, ?], C](Send(e, receiver +: other))

    def forward(e: Event, receiver: ProcessRef, other: ProcessRef*): Free[C, Unit] = Free.inject[FlowOp[F, ?], C](Forward(e, receiver +: other))

    // executes operations from the given flow in parallel
    def par(flow: Free[C, Unit], other: Free[C, Unit]*): Free[C, Unit] = Free.inject[FlowOp[F, ?], C](Par(flow +: other))

    // delays execution of each operation in the given flow
    // i.e. delay(duration, x~>p ++ y~>p) <-> delay(duration, x~>p) ++ delay(duration, y~>p) <-> delay(duration) ++ x~>p ++ delay(duration) ++ y~>p
    def delay(duration: FiniteDuration, flow: Free[C, Unit]): Free[C, Unit] = Free.inject[FlowOp[F, ?], C](Delay(duration, Some(flow)))

    // adds delays to the current flow, delays execution of any subsequent operation
    def delay(duration: FiniteDuration): Free[C, Unit] = Free.inject[FlowOp[F, ?], C](Delay(duration, None))

    def reply(f: ProcessRef => Free[C, Unit]): Free[C, Unit] = Free.inject[FlowOp[F, ?], C](Reply(f))

    def invoke(caller: ProcessRef, body: Free[C, Unit], callee: ProcessRef) : Free[C, Unit] =
      Free.inject[FlowOp[F, ?], C](Invoke(caller, body, callee))

    def fork(f: Free[C, Unit]): Free[C, Unit] =
      Free.inject[FlowOp[F, ?], C](Fork(f))

    def await[T <: Event](eventType: Class[T],
                           onTimeout: Free[C, Unit], timeout: FiniteDuration): Free[C, Unit] =
      await {
        case e if e.getClass.isAssignableFrom(eventType) =>
      }(onTimeout, timeout)

    def await(selector: PartialFunction[Event, Unit])
             (onTimeout: Free[C, Unit], timeout: FiniteDuration): Free[C, Unit] =
      Free.inject[FlowOp[F, ?], C](Await(selector, onTimeout, timeout))

  }

  object FlowOps {
    implicit def flowOps[F[_], G[_]](implicit I: InjectK[FlowOp[F, ?], G]): FlowOps[F, G] = new FlowOps[F, G]
  }

  // <----------- Effect ADT ----------->
  // Allows to use some other effect directly outside of Flow ADT, e.g. cats IO, Future, Task and etc.
  // must be compatible with Flow `F`
  sealed trait Effect[F[_], A]

  case class Suspend[F[_]](thunk: () => F[Unit]) extends Effect[F, Unit]

  case class Eval[F[_], A](thunk: () => A) extends Effect[F, Unit]

  // F - Effect type
  // C - coproduct of Effect and other algebras
  class Effects[F[_], C[_]](implicit I: InjectK[Effect[F, ?], C]) {
    // suspends an effect which produces `F`
    def suspend(thunk: => F[Unit]): Free[C, Unit] = Free.inject[Effect[F, ?], C](Suspend(() => thunk))

    // suspends a side effect in `F`
    def eval[A](thunk: => A): Free[C, Unit] = Free.inject[Effect[F, ?], C](Eval(() => thunk))
  }

  object Effects {
    implicit def effects[F[_], C[_]](implicit I: InjectK[Effect[F, ?], C]): Effects[F, C] = new Effects[F, C]
  }

  trait WithDsl[F[_]] {
    val flowDsl: FlowOps[F, Dsl[F, ?]] = implicitly[FlowOps[F, Dsl[F, ?]]]
    val effectDsl: Effects[F, Dsl[F, ?]] = implicitly[Effects[F, Dsl[F, ?]]]
  }

}
