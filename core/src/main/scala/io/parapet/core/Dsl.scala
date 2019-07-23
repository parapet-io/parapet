package io.parapet.core

import cats.InjectK
import cats.data.EitherK
import cats.free.Free

import scala.concurrent.duration.FiniteDuration

object Dsl {

  //type <:<[F[_], G[_], A] = EitherK[F, G, A]
  //type FlowOpOrEffect[F[_], A] = EitherK[FlowOp[F, ?], Effect[F, ?], A]
  type Dsl[F[_], A] = FlowOp[F, A]
  type DslF[F[_], A] = Free[Dsl[F, ?], A]

  // <----------- Flow ADT ----------->
  sealed trait FlowOp[F[_], A]

  case class Empty[F[_]]() extends FlowOp[F, Unit]

  case class Send[F[_]](e: Event, receivers: Seq[ProcessRef]) extends FlowOp[F, Unit]

  case class Forward[F[_]](e: Event, receivers: Seq[ProcessRef]) extends FlowOp[F, Unit]

  case class Par[F[_], G[_]](flow: Free[G, Unit]) extends FlowOp[F, Unit]

  case class Delay[F[_], G[_]](duration: FiniteDuration, flow: Option[Free[G, Unit]]) extends FlowOp[F, Unit]

  case class Reply[F[_], G[_]](f: ProcessRef => Free[G, Unit]) extends FlowOp[F, Unit]

  case class Invoke[F[_], G[_]](caller: ProcessRef, body: Free[G, Unit], callee: ProcessRef) extends FlowOp[F, Unit]

  case class Fork[F[_], G[_]](flow: Free[G, Unit]) extends FlowOp[F, Unit]

  case class Register[F[_]](parent: ProcessRef, child: Process[F]) extends FlowOp[F, Unit]

  case class Race[F[_], C[_]](first: Free[C, Unit], second: Free[C, Unit]) extends FlowOp[F, Unit]

  case class Suspend[F[_], C[_], A](thunk: () => F[A], bind: Option[A => Free[C, Unit]]) extends FlowOp[F, Unit]

  case class Eval[F[_], C[_], A](thunk: () => A, bind: Option[A => Free[C, Unit]]) extends FlowOp[F, Unit]

  // F - effect type
  // C - coproduct of FlowOp and other algebras
  class FlowOps[F[_], C[_]](implicit I: InjectK[FlowOp[F, ?], C]) {
    val empty: Free[C, Unit] = Free.inject[FlowOp[F, ?], C](Empty())

    /**
      * Semantically this operator is equivalent with `flatMap` or `bind` operator.
      *
      * @param f
      * @return
      */
    def flow(f: => Free[C, Unit]): Free[C, Unit] = evalWith(())(_ => f)

    /**
      * Sends an event to one or more receivers.
      * Event must be delivered to all receivers in specified order.
      *
      * @param e        event to send
      * @param receiver the first receiver
      * @param other    the other receivers
      * @return Unit
      */
    def send(e: Event, receiver: ProcessRef, other: ProcessRef*): Free[C, Unit] =
      Free.inject[FlowOp[F, ?], C](Send(e, receiver +: other))

    def forward(e: Event, receiver: ProcessRef, other: ProcessRef*): Free[C, Unit] =
      Free.inject[FlowOp[F, ?], C](Forward(e, receiver +: other))

    /**
      * Executes operations from the given flow in parallel.
      *
      * Example:
      *
      * {{{ par(eval(print(1)) ++ eval(print(2))) }}}
      *
      * possible outputs: {{{ 12 or 21 }}}
      *
      * Note: since the following flow will be executed in parallel the second operation won't be delayed
      *
      * {{{ par(delay(duration) ++ eval(print(1))) }}}
      *
      * Instead  use {{{ par(delay(duration, eval(print(1)))) }}}
      *
      * @param flow the flow which operations should be executed in parallel.
      * @return Unit
      */
    def par(flow: Free[C, Unit]): Free[C, Unit] = Free.inject[FlowOp[F, ?], C](Par(flow))

    /**
      * Delays every operation in the given flow for the given duration.
      *
      * For sequential flows the flowing expressions are semantically equivalent:
      * {{{
      *   delay(duration, x~>p ++ y~>p) <-> delay(duration, x~>p) ++ delay(duration, y~>p)
      *   delay(duration, x~>p ++ y~>p) <-> delay(duration) ++ x~>p ++ delay(duration) ++ y~>p
      * }}}
      *
      * For parallel flows:
      *
      * {{{
      *    delay(duration, par(x~>p ++ y~>p)) <-> delay(duration) ++ par(x~>p ++ y~>p)
      * }}}
      *
      * @param duration is the time span to wait before executing flow operations
      * @param flow     the flow which operations should be delayed
      * @return Unit
      */
    def delay(duration: FiniteDuration, flow: Free[C, Unit]): Free[C, Unit] = Free.inject[FlowOp[F, ?], C](Delay(duration, Some(flow)))

    /**
      * Delays any operation that follows this operation.
      *
      * Example:
      *
      * {{{
      *   delay(duration) ++ eval(println("hello from the future"))
      * }}}
      *
      * @param duration is the time span to wait before executing next operation
      * @return Unit
      */
    def delay(duration: FiniteDuration): Free[C, Unit] = Free.inject[FlowOp[F, ?], C](Delay(duration, None))

    /**
      * Accepts a callback function that takes a sender reference and produces a new flow.
      *
      * The code below will print `echo-hello` :
      * {{{
      *
      * val server: Process[IO] = new Process[IO] {
      *   override def handle: Receive = {
      *     case Request(data) => reply(sender => Response(s"echo-$data") ~> sender)
      *   }
      * }
      *
      * val client: Process[IO] = new Process[IO] {
      *   override def handle: Receive = {
      *     case Start => Request("hello") ~> server
      *     case res: Response => eval(eventStore.add(selfRef, res))
      *   }
      * }
      *
      * }}}
      *
      * @param f a callback function
      * @return Unit
      */
    def reply(f: ProcessRef => Free[C, Unit]): Free[C, Unit] = Free.inject[FlowOp[F, ?], C](Reply(f))

    def invoke(caller: ProcessRef, body: Free[C, Unit], callee: ProcessRef): Free[C, Unit] =
      Free.inject[FlowOp[F, ?], C](Invoke(caller, body, callee))

    def fork(f: Free[C, Unit]): Free[C, Unit] = Free.inject[FlowOp[F, ?], C](Fork(f))

    def register(parent: ProcessRef, child: Process[F]): Free[C, Unit] = Free.inject[FlowOp[F, ?], C](Register(parent, child))

    def race(first: Free[C, Unit], second: Free[C, Unit]): Free[C, Unit] = Free.inject[FlowOp[F, ?], C](Race(first, second))

    // adds an effect which produces `F` to this flow
    def suspend[A](thunk: => F[A]): Free[C, Unit] = Free.inject[FlowOp[F, ?], C](Suspend(() => thunk, Option.empty))

    def suspendWith[A](thunk: => F[A])(f: A => Free[C, Unit]): Free[C, Unit] =
      Free.inject[FlowOp[F, ?], C](Suspend(() => thunk, Option(f)))

    // suspends a side effect in `F`
    def eval[A](thunk: => A): Free[C, Unit] = Free.inject[FlowOp[F, ?], C](Eval(() => thunk, Option.empty))

    def evalWith[A](thunk: => A)(bind: A => Free[C, Unit]): Free[C, Unit] =
      Free.inject[FlowOp[F, ?], C](Eval(() => thunk, Option(bind)))

  }

  object FlowOps {
    implicit def flowOps[F[_], G[_]](implicit I: InjectK[FlowOp[F, ?], G]): FlowOps[F, G] = new FlowOps[F, G]
  }

  // <----------- Effect ADT ----------->
  // Allows to use some other effect directly outside of Flow ADT, e.g. cats IO, Future, Task and etc.
  // must be compatible with Flow `F`
  //sealed trait Effect[F[_], A]

  trait WithDsl[F[_]] {
    protected val dsl: FlowOps[F, Dsl[F, ?]] = implicitly[FlowOps[F, Dsl[F, ?]]]
  }

}
