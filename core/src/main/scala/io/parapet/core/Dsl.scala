package io.parapet.core

import cats.InjectK
import cats.free.Free
import io.parapet.core.annotations.developerApi
import io.parapet.{Event, ProcessRef}
import cats.implicits._

import scala.concurrent.duration.FiniteDuration

object Dsl {

  type Dsl[F[_], A] = FlowOp[F, A]
  type DslF[F[_], A] = Free[Dsl[F, *], A]

  // <----------- Flow ADT ----------->
  sealed trait FlowOp[F[_], A]

  case class UnitFlow[F[_]]() extends FlowOp[F, Unit]

  case class Pure[F[_], A](a: A) extends FlowOp[F, A]

  case class Send[F[_]](e: () => Event, sender: Option[ProcessRef],
                        receiver: ProcessRef, receivers: Seq[ProcessRef]) extends FlowOp[F, Unit]

  case class Forward[F[_]](e: () => Event, receivers: Seq[ProcessRef]) extends FlowOp[F, Unit]

  case class Par[F[_], G[_]](flow: Free[G, Unit]) extends FlowOp[F, Unit]

  case class Delay[F[_]](duration: FiniteDuration) extends FlowOp[F, Unit]

  case class WithSender[F[_], G[_], A](f: ProcessRef => Free[G, A]) extends FlowOp[F, A]

  case class Fork[F[_], G[_], A](flow: Free[G, A]) extends FlowOp[F, Fiber[F, A]]

  case class Register[F[_]](parent: ProcessRef, child: Process[F]) extends FlowOp[F, Unit]

  case class Race[F[_], C[_], A, B](first: Free[C, A], second: Free[C, B]) extends FlowOp[F, Either[A, B]]

  case class Suspend[F[_], C[_], A](thunk: () => F[A]) extends FlowOp[F, A]

  case class SuspendF[F[_], C[_], A](thunk: () => Free[C, A]) extends FlowOp[F, A]

  case class Eval[F[_], C[_], A](thunk: () => A) extends FlowOp[F, A]

  case class Blocking[F[_], C[_], A](body: () => Free[C, A]) extends FlowOp[F, Unit]

  case class RaiseError[A, F[_], G[_]](err: Throwable) extends FlowOp[F, A]

  case class HandelError[F[_], C[_], A, AA >: A](body: () => Free[C, A],
                                                 handle: Throwable => Free[C, AA]) extends FlowOp[F, AA]

  case class Halt[F[_]](ref: ProcessRef) extends FlowOp[F, Unit]

  case class Guarantee[F[_], C[_], A](fa: () => Free[C, A],
                                      finalizer: () => Free[C, Unit]) extends FlowOp[F, Unit]

  case class Lock[F[_]](ref: ProcessRef) extends FlowOp[F, Unit]

  case class Unlock[F[_]](ref: ProcessRef) extends FlowOp[F, Unit]

  /** Smart constructors for FlowOp[F, _].
    *
    * @param I an injection from type constructor `F` into type constructor `C`
    * @tparam F an effect type
    * @tparam C a coproduct of FlowOp and other algebras
    */
  class FlowOps[F[_], C[_]](implicit I: InjectK[FlowOp[F, *], C]) {

    /**
      * Semantically this operator is equivalent to `Monad.unit` and obeys the same laws.
      *
      * The following expressions are equivalent:
      * {{{
      *   event ~> process <-> unit ++ event ~> process
      *   event ~> process <-> event ~> process ++ unit
      * }}}
      */
    val unit: Free[C, Unit] = Free.inject[FlowOp[F, *], C](UnitFlow())

    /**
      * Lifts a value to F[_].
      *
      * Example:
      *
      * {{{
      *  for {
      *   a <- pure(1)
      *   _ <- eval(println(a))
      *  } yield ()
      * }}}
      *
      * @param a the value of type [[A]]
      * @tparam A a value type
      * @return the value of type [[A]]
      */
    def pure[A](a: A): Free[C, A] = Free.inject[FlowOp[F, *], C](Pure(a))

    /**
      * Suspends the given flow. Semantically this operator is equivalent to `suspend` for effects.
      * This is useful when implementing recursive flows.
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
      *    case Start => times(10000000)
      *  })
      *
      * }}}
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

    /**
      * Lazily constructs and sends an event to a one or more receivers.
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
      Free.inject[FlowOp[F, *], C](Send(() => e, Option.empty, receiver, other))

    /**
      * This function is equivalent to [[FlowOps.send]].
      * The main difference is that it overrides the sender process.
      */
    @developerApi
    def send(sender: ProcessRef, e: => Event, receiver: ProcessRef, other: ProcessRef*): Free[C, Unit] =
      Free.inject[FlowOp[F, *], C](Send(() => e, Option(sender), receiver, other))

    /**
      * Sends an event to the receiver using original sender reference.
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
      * console output: {{{client-proxy-ping}}}
      *
      * @param e        the event to send
      * @param receiver the receiver
      * @param other    optional receivers
      * @return Unit
      */
    def forward(e: => Event, receiver: ProcessRef, other: ProcessRef*): Free[C, Unit] =
      Free.inject[FlowOp[F, *], C](Forward(() => e, receiver +: other))

    /**
      * Executes operations from the given flow in parallel.
      *
      * Example:
      *
      * {{{ par(eval(print(1)) ++ eval(print(2))) }}}
      *
      * possible outputs: {{{ 12 or 21 }}}
      *
      * @param flows the flow which operations should be executed in parallel.
      * @return a list of [[Fiber]]
      */
    def par[A](flows: Free[C, A]*): Free[C, List[Fiber[F, A]]] =
      flows.toList.map(fork).sequence // .fold(unit)((a, b) => a.flatMap(_ => b))

    /**
      * Delays any operation that follows this flow.
      *
      * Example:
      *
      * {{{
      *   delay(duration) ++ eval(println("hello from the future"))
      * }}}
      *
      * After some duration "hello from the future" will be printed to the console.
      *
      * @param duration is the time span to wait before executing next operation
      * @return Unit
      */
    def delay(duration: FiniteDuration): Free[C, Unit] = Free.inject[FlowOp[F, *], C](Delay(duration))

    /**
      * Accepts a callback function that takes a sender reference and produces a new flow.
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

    /**
      * Executes the given flow asynchronously.
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
      * In order to get the result from asynchronous computation use [[Fiber.join]].
      *
      * Example:
      *
      * for {
      * fiber <- fork(eval("long running operation"))
      * res <- fiber.join
      * _ <- eval(print(res))
      * } yield ()
      *
      * {{{
      *   print to console: long running operation
      * }}}
      *
      * @param flow the flow to run asynchronously
      * @return Unit
      */
    def fork[A](flow: Free[C, A]): Free[C, Fiber[F, A]] = Free.inject[FlowOp[F, *], C](Fork(flow))

    /**
      * Registers a list of child processes.
      *
      * @param parent the parent process
      * @param child  the list of processes
      * @return Unit
      */
    def register(parent: ProcessRef, child: Process[F]*): Free[C, Unit] = {
      child.map(c => Free.inject[FlowOp[F, *], C](Register(parent, c)))
        .foldLeft(unit)((res, a) => res.flatMap(_ => a))
    }

    /**
      * Runs two flows concurrently. The loser of the race is canceled.
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

    /**
      * Suspends an effect which produces [[F]].
      *
      * {{{ suspend(IO(print("hi"))) }}}
      *
      * @param thunk an effect
      * @tparam A value type
      * @return a flow that produces a value of type [[A]]
      */
    def suspend[A](thunk: => F[A]): Free[C, A] = Free.inject[FlowOp[F, *], C](Suspend(() => thunk))

    /**
      * Suspends a side effect in [[F]].
      *
      * @param thunk a side effect
      * @tparam A value type
      * @return a flow that produces a value of type [[A]]
      */
    def eval[A](thunk: => A): Free[C, A] = Free.inject[FlowOp[F, *], C](Eval(() => thunk))

    /**
      * Asynchronously executes a flow produced by the given thunk w/o blocking a worker.
      * The main purpose of this operator is to tell Parapet
      * that the given flow can take indefinite amount of time complete.
      *
      * Example:
      * {{{
      *   class BlockingProcess extends Process[IO] {
      *     override def handle: Receive = {
      *       case Start => blocking(eval(while (true) {})) ++ eval(println("now"))
      *     }
      *   }
      * }}}
      *
      * Note: never write a code like this:
      *
      * {{{
      *   eval {
      *     while (true) {}
      *   }
      * }}}
      *
      * it was used only to demonstrate how this operator works.
      *
      * output {{{ now }}}
      *
      * Note: there is a significant difference between [[fork]] and [[blocking]].
      * fork is "fire and forget" with an optional blocking via [[Fiber.join]],
      * i.e. a process will be available for new events;
      * however, when using blocking it wont receive any events until an operation withing blocking is completed.
      *
      * @param thunk blocking code
      * @return Unit
      */
    def blocking[A](thunk: => Free[C, A]): Free[C, Unit] =
      Free.inject[FlowOp[F, *], C](Blocking(() => thunk))

    /**
      * Lifts an error to the current flow. Any subsequent operation won't be executed.
      *
      * @param err error
      * @return unit flow
      */
    def raiseError[A](err: Throwable): Free[C, A] = Free.inject[FlowOp[F, *], C](RaiseError(err))

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

    /**
      * Guaranteed to run finalizer after fa, even if fa completes with an error.
      *
      * Example:
      * {{{
      *  eval(throw new RuntimeException("error")).guarantee(eval(println("fallback")))
      *  console output: fallback
      *  error: java.lang.RuntimeException: error
      * }}}
      *
      * @param fa        the flow that produces a value of type [[A]]
      * @param finalizer the flow that is guaranteed to run
      * @tparam A type of value produced by fa
      * @return a flow that produces [[Unit]]
      */
    def guarantee[A](fa: => Free[C, A], finalizer: => Free[C, Unit]): Free[C, Unit] =
      Free.inject[FlowOp[F, *], C](Guarantee(() => fa, () => finalizer))

    /**
      * Destroys a child process.
      */
    def halt(ref: ProcessRef): Free[C, Unit] = Free.inject[FlowOp[F, *], C](Halt(ref))

    /**
      * Lock the given process.
      * The process won't receive any events until [[unlock]] is called for the process.
      *
      * @param ref the process ref
      * @return [[Unit]]
      */
    def lock(ref: ProcessRef): Free[C, Unit] = Free.inject[FlowOp[F, *], C](Lock(ref))

    /**
      * Unlocks the given process.
      *
      * @param ref the process ref
      * @return [[Unit]]
      */
    def unlock(ref: ProcessRef): Free[C, Unit] = Free.inject[FlowOp[F, *], C](Unlock(ref))
  }

  object FlowOps {

    type Aux[F[_]] = FlowOps[F, Dsl[F, *]]

    implicit def flowOps[F[_], G[_]](implicit I: InjectK[FlowOp[F, *], G]): FlowOps[F, G] = new FlowOps[F, G]
  }

  trait WithDsl[F[_]] {
    protected val dsl: FlowOps[F, Dsl[F, *]] = implicitly[FlowOps[F, Dsl[F, *]]]
  }

}
