package io.parapet.core

import cats.InjectK
import cats.free.Free

import scala.concurrent.duration.FiniteDuration

object Dsl {

  type Dsl[F[_], A] = FlowOp[F, A]
  type DslF[F[_], A] = Free[Dsl[F, *], A]

  // <----------- Flow ADT ----------->
  sealed trait FlowOp[F[_], A]

  case class UnitFlow[F[_]]() extends FlowOp[F, Unit]

  case class Send[F[_]](e: () => Event, receiver: ProcessRef, receivers: Seq[ProcessRef]) extends FlowOp[F, Unit]

  case class Forward[F[_]](e: () => Event, receivers: Seq[ProcessRef]) extends FlowOp[F, Unit]

  case class Par[F[_], G[_]](flow: Free[G, Unit]) extends FlowOp[F, Unit]

  case class Delay[F[_]](duration: FiniteDuration) extends FlowOp[F, Unit]

  case class WithSender[F[_], G[_], A](f: ProcessRef => Free[G, A]) extends FlowOp[F, A]

  case class Fork[F[_], G[_]](flow: Free[G, Unit]) extends FlowOp[F, Unit]

  case class Register[F[_]](parent: ProcessRef, child: Process[F]) extends FlowOp[F, Unit]

  case class Race[F[_], C[_], A, B](first: Free[C, A], second: Free[C, B]) extends FlowOp[F, Either[A, B]]

  case class Suspend[F[_], C[_], A](thunk: () => F[A]) extends FlowOp[F, A]

  case class SuspendF[F[_], C[_], A](thunk: () => Free[C, A]) extends FlowOp[F, A]

  case class Eval[F[_], C[_], A](thunk: () => A) extends FlowOp[F, A]

  case class Blocking[F[_], C[_], A](body: () => Free[C, A]) extends FlowOp[F, Unit]

  case class HandelError[F[_], C[_], A, AA >: A](body: () => Free[C, A], handle: Throwable => Free[C, AA]) extends FlowOp[F, AA]

  /** Smart constructors for FlowOp[F, _].
    *
    * @param I an injection from type constructor `F` into type constructor `C`
    * @tparam F an effect type
    * @tparam C a coproduct of FlowOp and other algebras
    */
  class FlowOps[F[_], C[_]](implicit I: InjectK[FlowOp[F, *], C]) {

    /** Semantically this operator is equivalent with `Monad.unit` and obeys the same laws.
      *
      * The following expressions are equivalent:
      * {{{
      *   event ~> process <-> unit ++ event ~> process
      *   event ~> process <-> event ~> process ++ unit
      * }}}
      */
    val unit: Free[C, Unit] = Free.inject[FlowOp[F, *], C](UnitFlow())

    /** Suspends the given flow. Semantically this operator is equivalent with `suspend` for effects.
      * This is useful for recursive flows.
      *
      * Recursive flow example for some `F[_]`:
      *
      * {{{
      *  def times[F[_]](n: Int) = {
      *    def step(remaining: Int): DslF[F, Unit] = flow {
      *      if (remaining == 0) unit
      *      else eval(print(remaining)) ++ step(remaining - 1)
      *    }
      *
      *    step(n)
      *  }
      *
      *  val process = Process[F](_ => {
      *    case Start => times(5)
      *  })
      *
      * }}}
      *
      * The code above will print {{{ 12345 }}}
      *
      * Note: it's strongly not recommended to perform any side effects within `flow` operator:
      *
      * NOT RECOMMENDED:
      *
      * {{{
      * def print(str: String) = flow {
      *   println(str)
      *   unit
      * }
      * }}}
      *
      * RECOMMENDED:
      * {{{
      * def print(str: String) = flow {
      *   eval(println(str))
      * }
      * }}}
      *
      * @param f a flow to suspend
      * @return Unit
      */
    def flow[A](f: => Free[C, A]): Free[C, A] = Free.inject[FlowOp[F, *], C](SuspendF(() => f))

    /** Lazily constructs and sends an event to one or more receivers.
      * Event must be delivered to all receivers in the specified order.
      *
      * Example:
      *
      * {{{
      *   send(Ping, processA, processB, processC)
      * }}}
      *
      * `Ping` event will be sent to the `processA` then `processB` and finally `processC`.
      * It's not guaranteed that `processA` will receive `Ping` event before `processC`
      * as it depends on it's processing speed and the current workload.
      *
      * @param e        event to send
      * @param receiver the receiver
      * @param other    optional receivers
      * @return Unit
      */
    def send(e: => Event, receiver: ProcessRef, other: ProcessRef*): Free[C, Unit] =
      Free.inject[FlowOp[F, *], C](Send(() => e, receiver, other))

    /** Sends an event to the receiver using original sender reference.
      * This is useful for implementing a proxy process.
      *
      * Proxy example for some `F[_]`:
      *
      * {{{
      * val server = Process[F](_ => {
      *   case Request(body) => withSender(sender => eval(println(s"$sender-$body")))
      * })
      *
      * val proxy = Process[F](_ => {
      *   case Request(body) => forward(Request(s"proxy-$body"), server.ref)
      * })
      *
      * val client = Process.builder[F](_ => {
      *   case Start => Request("ping") ~> proxy
      * }).ref(ProcessRef("client")).build
      * }}}
      *
      * The code above will print: `client-proxy-ping`
      *
      * @param e        the event to send
      * @param receiver the receiver
      * @param other    optional receivers
      * @return Unit
      */
    def forward(e: => Event, receiver: ProcessRef, other: ProcessRef*): Free[C, Unit] =
      Free.inject[FlowOp[F, *], C](Forward(() => e, receiver +: other))

    /** Executes operations from the given flow in parallel.
      *
      * Example:
      *
      * {{{ par(eval(print(1)) ++ eval(print(2))) }}}
      *
      * possible outputs: {{{ 12 or 21 }}}
      *
      * @param flows the flow which operations should be executed in parallel.
      * @return Unit
      */

    def par(flows: Free[C, Unit]*): Free[C, Unit] = flows.map(fork).fold(unit)((a, b) => a.flatMap(_ => b))

    /** Delays any operation that follows this operator.
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
    def delay(duration: FiniteDuration): Free[C, Unit] = Free.inject[FlowOp[F, *], C](Delay(duration))

    /** Accepts a callback function that takes a sender reference and produces a new flow.
      *
      * The code below will print `client says hello` :
      * {{{
      *
      * val server = Process[F](_ => {
      *   case Request(data) => withSender(sender => eval(print(s"$sender says $data")))
      * })
      *
      * val client = Process.builder[F](_ => {
      *   case Start => Request("hello") ~> server
      * }).ref(ProcessRef("client")).build
      *
      * }}}
      *
      * @param f a callback function
      * @tparam A value type
      * @return a value
      */
    def withSender[A](f: ProcessRef => Free[C, A]): Free[C, A] = Free.inject[FlowOp[F, *], C](WithSender(f))

    /** Executes the given flow concurrently.
      *
      * Example:
      *
      * {{{
      * val process = Process[F](_ => {
      *   case Start => fork(eval(print(1))) ++ fork(eval(print(2)))
      * })
      * }}}
      *
      * Possible outputs: {{{  12 or 21  }}}
      *
      * NOTE: it's a fire and forget operator, a process can receive new events immediately
      *
      * @param flow the flow to run concurrently
      * @return Unit
      */
    def fork(flow: Free[C, Unit]): Free[C, Unit] = Free.inject[FlowOp[F, *], C](Fork(flow))

    /** Registers a child process in the parapet context.
      *
      * @param parent the parent process
      * @param child  the child process
      * @return Unit
      */
    def register(parent: ProcessRef, child: Process[F]): Free[C, Unit] =
      Free.inject[FlowOp[F, *], C](Register(parent, child))

    /** Runs two flows concurrently. The loser of the race is canceled.
      *
      * Example:
      *
      * {{{
      * val forever = eval(while (true) {})
      *
      * val process: Process[F] = Process[F](_ => {
      *   case Start => race(forever, eval(println("winner")))
      * })
      * }}}
      *
      * Output: `winner`
      *
      * @param first  the first flow
      * @param second the second flow
      * @return either[A, B]
      */
    def race[A, B](first: Free[C, A], second: Free[C, B]): Free[C, Either[A, B]] =
      Free.inject[FlowOp[F, *], C](Race(first, second))

    /** Adds an effect which produces `F` to the current flow.
      *
      * {{{ suspend(IO(print("hi"))) }}}
      *
      * @param thunk an effect
      * @tparam A value type
      * @return value
      */
    def suspend[A](thunk: => F[A]): Free[C, A] = Free.inject[FlowOp[F, *], C](Suspend(() => thunk))

    /** Suspends a side effect in `F` and then adds that to the current flow.
      *
      * @param thunk a side effect
      * @tparam A value type
      * @return value
      */
    def eval[A](thunk: => A): Free[C, A] = Free.inject[FlowOp[F, *], C](Eval(() => thunk))

    /** Asynchronously executes a flow produced by the given thunk w/o blocking a worker.
      *
      * Example:
      * {{{
      *
      *   class BlockingProcess extends Process[IO] {
      *     override def handle: Receive = {
      *       case Start => blocking(eval(while (true) {})) ++ eval(println("now"))
      *     }
      *   }
      *
      * }}}
      *
      * output {{{ now }}}
      *
      * Note: there is a significant difference between fork and blocking. fork is fire and forget ,
      * i.e. a process will be available for new events; however, when using blocking it wont receive any events until an
      * operation withing blocking is completed.
      *
      * @param thunk blocking code
      * @return Unit
      */
    def blocking[A](thunk: => Free[C, A]): Free[C, Unit] =
      Free.inject[FlowOp[F, *], C](Blocking(() => thunk))

    /**
      * Handle any error, potentially recovering from it, by mapping it to an Free[C, AA] value.
      *
      * @param thunk  a flow to execute
      * @param handle error handler
      * @tparam A  result
      * @tparam AA contravariant to A
      * @return result of thunk or handle in case of any error
      */
    def handleError[A, AA >: A](thunk: => Free[C, A], handle: Throwable => Free[C, AA]): Free[C, AA] = {
      Free.inject[FlowOp[F, *], C](HandelError(() => thunk, handle))
    }
  }

  object FlowOps {
    implicit def flowOps[F[_], G[_]](implicit I: InjectK[FlowOp[F, *], G]): FlowOps[F, G] = new FlowOps[F, G]
  }

  trait WithDsl[F[_]] {
    protected val dsl: FlowOps[F, Dsl[F, *]] = implicitly[FlowOps[F, Dsl[F, *]]]
  }

}
