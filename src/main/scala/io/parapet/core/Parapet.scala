package io.parapet.core

import java.util.concurrent.atomic.AtomicInteger

import cats.data.{EitherK, State}
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import cats.free.Free
import cats.{InjectK, Monad}
import io.parapet.core.Parapet.CatsProcess

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.language.{higherKinds, implicitConversions, reflectiveCalls}
import cats.effect.IO._
import cats.implicits._
import cats.~>
import scala.concurrent.duration._
//import fs2.concurrent.{Queue => CQueue}
//
//import scala.collection.mutable.{Queue => SQueue}

// todo define failure scenarios
// todo Send failed message to dead letter queue
// todo add Lock
// todo integration tests
object Parapet {

  type <:<[F[_], G[_], A] = EitherK[F, G, A]
  type FlowOpOrEffect[F[_], A] = <:<[FlowOp[F, ?], Effect[F, ?], A]
  type FlowF[F[_], A] = Free[FlowOpOrEffect[F, ?], A]
  type FlowState[F[_], A] = State[Seq[F[_]], A]

  class Interpreter[F[_]](interpreter: FlowOpOrEffect[F, ?] ~> FlowState[F, ?]) {
    def interpret(program: FlowF[F, Unit]): Seq[F[_]] =
      program.foldMap[FlowState[F, ?]](interpreter)
        .runS(ListBuffer()).value
  }

  trait Event

  case class Message(sender: String, payload: String) extends Event

  implicit class EventOps[F[_]](e: => Event) {

    def ~>(process: Process[F])(implicit FL: Flow[F, FlowOpOrEffect[F, ?]]): FlowF[F, Unit] = FL.send(e, process) //?
  }

  trait Queue[F[_], A] {
    def enqueue(e: => A): F[Unit]

    def dequeue: F[A]
  }

  object Queue {
    def bounded[F[_] : Concurrent, A](capacity: Int): F[Queue[F, A]] = {
      fs2.concurrent.Queue.bounded[F, A](capacity).map(q => new Queue[F, A] {
        override def enqueue(e: => A): F[Unit] =
           q.enqueue1(e) *> implicitly[Concurrent[F]].delay(println("submitted task"))

        override def dequeue: F[A] = q.dequeue1
      })
    }
  }

  trait QueueModule[F[_], A] {
    def queue: Queue[F, A]
  }

  trait TaskQueueModule[F[_]] {
    def taskQueue: Queue[F, Task[F]]
  }

  trait Parallel[F[_]] {
    // runs given effects in parallel and returns a single effect
    def par(effects: Seq[F[_]]): F[Unit]
  }

  def ioParallel(implicit ctx: ContextShift[IO]): Parallel[IO] = (effects: Seq[IO[_]]) => effects.toList.parSequence_

  // todo implement RetryStrategy
  trait Retry[F[_]] {
    def retryWithBackoff[A](fa: F[A], initialDelay: FiniteDuration, maxRetries: Int, backoffBase: Int = 2): F[A]
  }

  class IORetry(timer: Timer[IO]) extends Retry[IO] {
    override def retryWithBackoff[A](ioa: IO[A],
                                     initialDelay: FiniteDuration,
                                     maxRetries: Int, backoffBase: Int = 2): IO[A] = {
      ioa.handleErrorWith { error =>
        if (maxRetries > 0)
          IO.sleep(initialDelay)(timer) *> /*IO(println(s"retry: $maxRetries")) *>*/ retryWithBackoff(ioa, initialDelay * backoffBase, maxRetries - 1)
        else
          IO.raiseError(error)
      }
    }
  }

  // <----------- Schedule ----------->
  case class Task[F[_]](event: () => Event, p: Process[F])
  type TaskQueue[F[_]] = F[Queue[F, Task[F]]]

  class Scheduler[F[_]: Monad](queue: Queue[F, Task[F]],
                               numOfWorkers: Int,
                               parallel: Parallel[F],
                               interpreter: Interpreter[F],
                               retry: Retry[F]) {
    def submit(task: Task[F]): F[Unit] = queue.enqueue(task)

    def run: F[Unit] = {
      parallel.par((0 until numOfWorkers).map(i => new Worker(s"worker-$i", queue, parallel, interpreter, retry).run))
    }
  }

  trait SchedulerModule[F[_]] {
    val scheduler: Scheduler[F]
  }

  class Worker[F[_] : Monad](name: String,
                             queue: Queue[F, Task[F]],
                             parallel: Parallel[F],
                             interpreter: Interpreter[F],
                             retry: Retry[F]) {
    def process(task: Task[F]): F[Unit] = {
      val program: FlowF[F, Unit] = task.p.handle.apply(task.event())
      val res = interpreter.interpret(program).fold(Monad[F].unit)(_ *> _).void
      retry.retryWithBackoff(res, 0.seconds, 5, 0) // todo send to dead letter
    }

    def run: F[Unit] = {
      def step: F[Unit] =
        for {
          task <- queue.dequeue
          _ <- implicitly[Monad[F]].pure(println("dequeued task: " + task.event()))
          _ <- process(task)
          _ <- step
        } yield ()

      step
    }
  }

  // <----------- Flow ADT ----------->
  sealed trait FlowOp[F[_], A]
  case class Empty[F[_]]() extends FlowOp[F, Unit]
  case class Send[F[_]](f: () => Event, receivers: Seq[Process[F]]) extends FlowOp[F, Unit]
  case class Par[F[_], G[_]](flow: Free[G, Unit]) extends FlowOp[F, Unit]
  case class Delay[F[_], G[_]](duration: FiniteDuration, flow: Free[G, Unit]) extends FlowOp[F, Unit]


  // F - effect type
  // G - target program
  class Flow[F[_], G[_]](implicit I: InjectK[FlowOp[F, ?], G]) {
    val empty: Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Empty())

    // sends event `e` to the list of receivers
    def send(e: => Event, receiver: Process[F], other: Process[F]*): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Send(() => e, receiver +: other))

    // changes sequential execution to parallel
    def par(flow: Free[G, Unit]): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Par(flow))

    // delays execution of the given `flow`
    def delay(duration: FiniteDuration, flow: Free[G, Unit]): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Delay(duration, flow))
  }

  object Flow {
    implicit def flow[F[_], G[_]](implicit I: InjectK[FlowOp[F, ?], G]): Flow[F, G] = new Flow[F, G]
  }

  // <----------- Effect ADT ----------->
  // Allows to use some other effect directly outside of Flow ADT, e.g cats IO, Future, Task and etc.
  // must be compatible with Flow F
  sealed trait Effect[F[_], A]
  case class Suspend[F[_]](thunk: () => F[Unit]) extends Effect[F, Unit]
  case class Eval[F[_]](thunk: () => Unit) extends Effect[F, Unit]

  // F - effect type
  // G - target program
  class Effects[F[_], G[_]](implicit I: InjectK[Effect[F, ?], G]) {
    // suspends an effect which produces `F`
    def suspend(thunk: => F[Unit]): Free[G, Unit] =
      Free.inject[Effect[F, ?], G](Suspend(() => thunk))

    // suspends a side effect in `F`
    def eval(thunk: => Unit): Free[G, Unit] = {
      Free.inject[Effect[F, ?], G](Eval(() => thunk))
    }
  }


  //class IOEffects[F[_]](implicit I: InjectK[IOEffects.IOEffect, F]) extends Effects[IO, F]

  object Effects {
    //type IOEffect[A] = Effect[IO, A]

    implicit def effects[F[_], G[_]](implicit I: InjectK[Effect[F, ?], G]): Effects[F, G] = new Effects[F, G]
    //implicit def effects[G[_]](implicit I: InjectK[Effect[IO, ?], G]): Effects[IO, G] = new Effects[IO, G]
  }


  implicit class FreeOps[F[_], A](fa: Free[F, A]) {
    // alias for Free flatMap
    def ++[B](fb: Free[F, B]): Free[F, B] = fa.flatMap(_ => fb)
  }

  // Interpreters for ADTs based on cats IO
  type IOFlowOpOrEffect[A] = FlowOpOrEffect[IO, A]

  def ioFlowInterpreter[Env <: TaskQueueModule[IO] with CatsModule](env: Env): FlowOp[IO, ?] ~> FlowState[IO, ?] = new (FlowOp[IO, ?] ~> FlowState[IO, ?]) {

    def run[A](flow: FlowF[IO, A], interpreter: IOFlowOpOrEffect ~> FlowState[IO, ?]): Seq[IO[_]] = {
      flow.foldMap(interpreter).runS(ListBuffer()).value
    }

    override def apply[A](fa: FlowOp[IO, A]): FlowState[IO, A] = {
      implicit val ctx: ContextShift[IO] = env.ctx
      implicit val ioTimer: Timer[IO] = env.timer
      val parallel: Parallel[IO] = ioParallel
      val interpreter: IOFlowOpOrEffect ~> FlowState[IO, ?] = ioFlowInterpreter(env) or ioEffectInterpreter
      fa match {
        case Empty() => State.set(ListBuffer.empty)
        case Send(thunk, receivers) =>
          val ops = receivers.map(receiver => env.taskQueue.enqueue(Task(thunk, receiver)))
          State[Seq[IO[_]], Unit] { s => (s ++ ops, ()) }
        case Par(flow) =>
          val res = parallel.par(flow.asInstanceOf[FlowF[IO, A]]
            .foldMap(interpreter).runS(ListBuffer()).value)
          State[Seq[IO[_]], Unit] { s => (s :+ res, ()) }
        // todo: behavior needs to be determined for par / seq flow
        case Delay(duration, flow) =>
          val delayIO = IO.sleep(duration)
          val res = run(flow.asInstanceOf[FlowF[IO, A]], interpreter).map(op => delayIO *> op)
          State[Seq[IO[_]], Unit] { s => (s ++ res, ()) }
      }
    }
  }

  def ioEffectInterpreter: Effect[IO, ?] ~> FlowState[IO, ?] = new (Effect[IO, ?] ~> FlowState[IO, ?]) {
    override def apply[A](fa: Effect[IO, A]): FlowState[IO, A] = fa match {
      case Suspend(thunk) => State.modify[Seq[IO[_]]](s => s ++ Seq(thunk()))
      case Eval(thunk) => State.modify[Seq[IO[_]]](s => s ++ Seq(IO(thunk())))
    }
  }

  // <-------------- Process -------------->
  trait Process[F[_]] {
    self =>
    type ProcessFlow = FlowF[F, Unit] //  replaced  with FlowF[F]
    type Receive = PartialFunction[Event, ProcessFlow]

    val handle: Receive

    // composition of this and `pb` process
    def ++(pb: Process[F]): Process[F] = new Process[F] {
      override val handle: Receive = {
        case e => self.handle(e) ++ pb.handle(e)
      }
    }
  }

  object Process {
    def apply[F[_]](receive: PartialFunction[Event, FlowF[F, Unit]]): Process[F] = new Process[F] {
      override val handle: Receive = receive
    }
  }

  trait CatsProcess extends Process[IO]

  trait CatsModule {
    val ctx: ContextShift[IO]
    val timer: Timer[IO]
  }

  case class CatsAppEnv(
                         taskQueue: Queue[IO, Task[IO]],
                         ctx: ContextShift[IO],
                         timer: Timer[IO]) extends TaskQueueModule[IO] with CatsModule

  object CatsAppEnv {
    def apply(): IO[CatsAppEnv] = {
      implicit val ctx: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      for {
        taskQueue <- Queue.bounded[IO, Task[IO]](10)
      } yield CatsAppEnv(taskQueue, ctx, IO.timer(ExecutionContext.global))
    }
  }


  abstract class ParApp[F[+ _], Env <: TaskQueueModule[F]](implicit M: Monad[F]) {
    type EffectF[A] = Effect[F, A]
    type FlowOpF[A] = FlowOp[F, A]
    type FlowStateF[A] = FlowState[F, A]
    type ProcessFlow = FlowF[F, Unit]

    val env: F[Env]

    def flowInterpreter(e: Env): FlowOpF ~> FlowStateF

    def effectInterpreter(e: Env): EffectF ~> FlowStateF

    def parallel(env: Env): Parallel[F]

    def retry(env: Env): Retry[F]

    val numberOfWorkers = 1
    def program: ProcessFlow

    def run: F[Unit] => Unit

    def main(args: Array[String]): Unit = {
      val p = for {
        e <- env
        P <- M.pure(parallel(e))
        interpreter <- M.pure(new Interpreter[F](flowInterpreter(e) or effectInterpreter(e)))
        scheduler <-M.pure(new Scheduler[F](e.taskQueue, numberOfWorkers, P, interpreter, retry(e)))
        _ <- P.par(Seq(interpreter.interpret(program).toList.sequence_, scheduler.run))
      } yield ()
      run(p)
    }
  }

  abstract class CatsApp extends ParApp[IO, CatsAppEnv] {
    override val env: IO[CatsAppEnv] = CatsAppEnv()

    override def flowInterpreter(e: CatsAppEnv): FlowOpF ~> FlowStateF = ioFlowInterpreter(e)

    override def effectInterpreter(e: CatsAppEnv): EffectF ~> FlowStateF = ioEffectInterpreter

    override def parallel(env: CatsAppEnv): Parallel[IO] = ioParallel(env.ctx)

    override def retry(env: CatsAppEnv): Retry[IO] = new IORetry(env.timer)

    override def run: IO[Unit] => Unit = _.unsafeRunSync()
  }

//  // todo use cats effect queue
//  class IOQueue[A] extends Queue[IO, A] {
//    val queue: SQueue[A] = new SQueue()
//
//    override def enqueue(e: => A): IO[Unit] = IO(queue.enqueue(e))
//
//    override def dequeue: IO[A] = IO(queue.dequeue())
//  }

}

import io.parapet.core.Parapet.{CatsApp, Event}

object MyApp extends CatsApp {

  import CounterProcess._
  import io.parapet.core.catsInstances.flow._ // for Flow DSL
  import io.parapet.core.catsInstances.effect._ // for Effect DSL

  // Client program
  class CounterProcess extends CatsProcess /* Process[IO] */ {
    // state
    var counter = 0

    val handle: Receive = {
      case Inc =>
        eval {
          counter = counter + 1
        } ++ eval(println(s"counter=$counter"))
    }
  }

  object CounterProcess {

    // API
    object Inc extends Event

  }

  override def program: ProcessFlow = {
    val counter = new CounterProcess()
    Inc ~> counter ++ Inc ~> counter ++
      Inc ~> (counter ++ counter) ++ // compose two processes
      eval(println("===============")) ++ // print line
      par(eval(println("a")) ++ eval(println("b")) ++ eval(println("c")))
  }

  // possible outputs:
  //  counter=1
  //  counter=2
  //  counter=3
  //  counter=4
  //  ===============
  //  a
  //  b
  //  c
  //  ^ any permutation of a,b,c

}

object CounterApp extends CatsApp {

  import CounterProcess._
  import io.parapet.core.catsInstances.flow._ // for Flow DSL
  import io.parapet.core.catsInstances.effect._ // for Effect DSL
  import scala.concurrent.duration._
  class CounterProcess extends CatsProcess {
    val counter = new AtomicInteger()
    override val handle: Receive = {
      case Inc => delay(1.second, eval {
        counter.incrementAndGet()
      }) ++ eval(println(s"counter1=${counter.get()}"))
      case Print => suspend(IO.delay(println(s"counter2=${counter.get()}")))
    }
  }

  object CounterProcess {
    object Inc extends Event
    object Print extends Event
  }

  override def program: CounterApp.ProcessFlow = {
    val counter = new CounterProcess()
    eval(println("send Inc message to counter")) ++
      Inc ~> counter ++
      eval(println("send Print message to counter")) ++
      Print ~> counter
  }
}
