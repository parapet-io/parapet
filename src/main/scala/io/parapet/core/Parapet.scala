package io.parapet.core

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}

import cats.data.{EitherK, State}
import cats.effect.IO._
import cats.effect._
import cats.free.Free
import cats.implicits._
import cats.{InjectK, Monad, ~>}
import com.typesafe.scalalogging.StrictLogging
import io.parapet.core.Event._
import io.parapet.core.ProcessRef._
import io.parapet.core.Scheduler._
import io.parapet.core.annotations.experimental
import io.parapet.core.processes.{DeadLetterProcess, SystemProcess}
import io.parapet.instances.parallel._
import io.parapet.syntax.flow._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.language.{higherKinds, implicitConversions, reflectiveCalls}

object Parapet extends StrictLogging {

  type <:<[F[_], G[_], A] = EitherK[F, G, A]
  type FlowOpOrEffect[F[_], A] = <:<[FlowOp[F, ?], Effect[F, ?], A]
  type FlowF[F[_], A] = Free[FlowOpOrEffect[F, ?], A]
  type FlowStateF[F[_], A] = State[FlowState[F], A]
  type Interpreter[F[_]] = FlowOpOrEffect[F, ?] ~> FlowStateF[F, ?]

  val ParapetPrefix = "parapet"

  private[core] def interpret[F[_] : Monad, A](program: FlowF[F, A], interpreter: Interpreter[F], state: FlowState[F]): Seq[F[_]] = {
    program.foldMap[FlowStateF[F, ?]](interpreter).runS(state).value.ops
  }

  private[core] def interpret_[F[_] : Monad, A](program: FlowF[F, A], interpreter: Interpreter[F], state: FlowState[F]): F[Unit] = {
    interpret(program, interpreter, state).fold(Monad[F].unit)(_ >> _).void
  }


  case class FlowState[F[_]](senderRef: ProcessRef, selfRef: ProcessRef, ops: Seq[F[_]]) {
    def addOps(that: Seq[F[_]]): FlowState[F] = this.copy(ops = ops ++ that)
  }

  object FlowState {
    def apply[F[_]](senderRef: ProcessRef, selfRef: ProcessRef): FlowState[F] =
      FlowState(senderRef = senderRef, selfRef = selfRef, ops = ListBuffer())
  }

  // <----------- Flow ADT ----------->
  sealed trait FlowOp[F[_], A]

  case class Empty[F[_]]() extends FlowOp[F, Unit]

  case class Use[F[_], G[_], A](resource: () => A, flow: A => Free[G, Unit]) extends FlowOp[F, Unit]

  case class Send[F[_]](e: Event, receivers: Seq[ProcessRef]) extends FlowOp[F, Unit]

  case class Par[F[_], G[_]](flow: Seq[Free[G, Unit]]) extends FlowOp[F, Unit]

  case class Delay[F[_], G[_]](duration: FiniteDuration, flow: Option[Free[G, Unit]]) extends FlowOp[F, Unit]

  case class Stop[F[_]]() extends FlowOp[F, Unit]

  case class Reply[F[_], G[_]](f: ProcessRef => Free[G, Unit]) extends FlowOp[F, Unit]

  // F - effect type
  // G - target program
  class Flow[F[_], G[_]](implicit I: InjectK[FlowOp[F, ?], G]) {
    val empty: Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Empty())
    val terminate: Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Stop())

    def use[A](resource: => A)(f: A => Free[G, Unit]): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Use(() => resource, f))

    // sends event `e` to the list of receivers
    def send(e: Event, receiver: ProcessRef, other: ProcessRef*): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Send(e, receiver +: other))

    // executes operations from the given flow in parallel
    def par(flow: Free[G, Unit], other: Free[G, Unit]*): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Par(flow +: other))

    // delays execution of each operation in the given flow
    // i.e. delay(duration, x~>p ++ y~>p) <-> delay(duration, x~>p) ++ delay(duration, y~>p) <-> delay(duration) ++ x~>p ++ delay(duration) ++ y~>p
    def delay(duration: FiniteDuration, flow: Free[G, Unit]): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Delay(duration, Some(flow)))

    // adds delays to the current flow, delays execution of any subsequent operation
    def delay(duration: FiniteDuration): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Delay(duration, None))

    def reply(f: ProcessRef => Free[G, Unit]): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Reply(f))
  }

  object Flow {
    implicit def flow[F[_], G[_]](implicit I: InjectK[FlowOp[F, ?], G]): Flow[F, G] = new Flow[F, G]
  }

  // <----------- Effect ADT ----------->
  // Allows to use some other effect directly outside of Flow ADT, e.g. cats IO, Future, Task and etc.
  // must be compatible with Flow `F`
  sealed trait Effect[F[_], A]

  case class Suspend[F[_]](thunk: () => F[Unit]) extends Effect[F, Unit]

  case class Eval[F[_]](thunk: () => Unit) extends Effect[F, Unit]

  // F - effect type
  // G - target program
  class Effects[F[_], G[_]](implicit I: InjectK[Effect[F, ?], G]) {
    // suspends an effect which produces `F`
    def suspend(thunk: => F[Unit]): Free[G, Unit] = Free.inject[Effect[F, ?], G](Suspend(() => thunk))

    // suspends a side effect in `F`
    def eval(thunk: => Unit): Free[G, Unit] = Free.inject[Effect[F, ?], G](Eval(() => thunk))
  }

  object Effects {
    implicit def effects[F[_], G[_]](implicit I: InjectK[Effect[F, ?], G]): Effects[F, G] = new Effects[F, G]
  }


  // Interpreters for ADTs based on cats IO
  type IOFlowOpOrEffect[A] = FlowOpOrEffect[IO, A]

  def ioFlowInterpreter(taskQueue: TaskQueue[IO])(implicit ctx: ContextShift[IO], timer: Timer[IO]): FlowOp[IO, ?] ~> FlowStateF[IO, ?] = new (FlowOp[IO, ?] ~> FlowStateF[IO, ?]) {

    override def apply[A](fa: FlowOp[IO, A]): FlowStateF[IO, A] = {

      val parallel: Parallel[IO] = Parallel[IO]
      val interpreter: Interpreter[IO] = ioFlowInterpreter(taskQueue) or ioEffectInterpreter
      fa match {
        case Empty() => State[FlowState[IO], Unit] { s => (s, ()) }
        case Use(resource, f) => State[FlowState[IO], Unit] { s =>
          val res = IO.delay(resource()) >>= (r => interpret(f(r).asInstanceOf[FlowF[IO, A]], interpreter, s.copy(ops = ListBuffer())).toList.sequence)
          (s.addOps(Seq(res)), ())
        }
        case Stop() => State[FlowState[IO], Unit] { s => (s.addOps(Seq(taskQueue.enqueue(Terminate()))), ()) }
        case Send(event, receivers) =>
          State[FlowState[IO], Unit] { s =>
            val ops = receivers.map(receiver => taskQueue.enqueue(Deliver(Envelope(s.selfRef, event, receiver))))
            (s.addOps(ops), ())
          }
        case Par(flows) =>
          State[FlowState[IO], Unit] { s =>
            val res = parallel.par(
              flows.map(flow => interpret_(flow.asInstanceOf[FlowF[IO, A]], interpreter, s.copy(ops = ListBuffer()))))
            (s.addOps(Seq(res)), ())
          }
        case Delay(duration, Some(flow)) =>
          State[FlowState[IO], Unit] { s =>
            val delayIO = IO.sleep(duration)
            val res = interpret(flow.asInstanceOf[FlowF[IO, A]], interpreter, s.copy(ops = ListBuffer())).map(op => delayIO >> op)
            (s.addOps(res), ())
          }
        case Delay(duration, None) =>
          State[FlowState[IO], Unit] { s => (s.addOps(Seq(IO.sleep(duration))), ()) }

        case Reply(f) =>
          State[FlowState[IO], Unit] { s =>
            (
              s.addOps(interpret(f(s.senderRef).asInstanceOf[FlowF[IO, A]], interpreter, s.copy(ops = ListBuffer()))),
              ()
            )
          }
      }
    }
  }

  def ioEffectInterpreter: Effect[IO, ?] ~> FlowStateF[IO, ?] = new (Effect[IO, ?] ~> FlowStateF[IO, ?]) {
    override def apply[A](fa: Effect[IO, A]): FlowStateF[IO, A] = fa match {
      case Suspend(thunk) => State.modify[FlowState[IO]](s => s.addOps(Seq(thunk())))
      case Eval(thunk) => State.modify[FlowState[IO]](s => s.addOps(Seq(IO(thunk()))))
    }
  }

  trait CatsModule {
    val ctx: ContextShift[IO]
    val timer: Timer[IO]
  }

  case class CatsAppEnv(ctx: ContextShift[IO],
                        timer: Timer[IO]) extends CatsModule


  case class ParConfig(schedulerConfig: SchedulerConfig)

  @experimental
  case class AppContext[F[_]](taskQueue: TaskQueue[F])

  abstract class ParApp[F[+ _]] {
    type EffectF[A] = Effect[F, A]
    type FlowOpF[A] = FlowOp[F, A]
    type FlowStateA[A] = FlowStateF[F, A]
    type ProcessFlow = FlowF[F, Unit]

    val config: ParConfig = ParApp.defaultConfig
    implicit val parallel: Parallel[F]
    implicit val timer: Timer[F]
    implicit val concurrentEffect: ConcurrentEffect[F]
    implicit val contextShift: ContextShift[F]
    val processes: Array[Process[F]]

    // system processes
    def deadLetter: DeadLetterProcess[F] = DeadLetterProcess.logging

    private[core] lazy val systemProcess: Process[F] = new SystemProcess[F]

    def flowInterpreter(taskQueue: TaskQueue[F]): FlowOpF ~> FlowStateA

    def effectInterpreter: EffectF ~> FlowStateA

    val program: ProcessFlow

    def unsafeRun(f: F[Unit]): Unit

    def stop: F[Unit]

    private[core] final def initProcesses(implicit F: Flow[F, FlowOpOrEffect[F, ?]]): Free[FlowOpOrEffect[F, ?], Unit] =
      processes.map(p => F.send(Start, p.ref)).foldLeft(F.empty)(_ ++ _)

    def run: F[AppContext[F]] = {
      if (processes.isEmpty) {
        concurrentEffect.raiseError(new RuntimeException("Initialization error:  at least one process must be provided"))
      } else {
        val systemProcesses = Array(systemProcess, deadLetter)
        for {
          taskQueue <- Queue.bounded[F, Task[F]](config.schedulerConfig.queueSize)
          context <- concurrentEffect.pure(AppContext(taskQueue))
          interpreter <- concurrentEffect.pure(flowInterpreter(taskQueue) or effectInterpreter)
          scheduler <- Scheduler[F](config.schedulerConfig, systemProcesses ++ processes, taskQueue, interpreter)
          _ <- parallel.par(
            Seq(interpret_(initProcesses ++ program, interpreter, FlowState(SystemRef, SystemRef)),
              scheduler.run))
          _ <- stop
        } yield context
      }
    }

    def main(args: Array[String]): Unit = {
      unsafeRun(run.void)
    }
  }

  abstract class CatsApp extends ParApp[IO] {
    val executorService: ExecutorService =
      Executors.newFixedThreadPool(
        Runtime.getRuntime.availableProcessors(), new ThreadFactory {
          val threadNumber = new AtomicInteger(1)

          override def newThread(r: Runnable): Thread =
            new Thread(r, s"$ParapetPrefix-thread-${threadNumber.getAndIncrement()}")
        })

    implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(executorService)
    implicit val contextShift: ContextShift[IO] = IO.contextShift(executionContext)
    override val parallel: Parallel[IO] = Parallel[IO]
    override val timer: Timer[IO] = IO.timer(executionContext)
    override val concurrentEffect: ConcurrentEffect[IO] = implicitly[ConcurrentEffect[IO]]

    override def flowInterpreter(taskQueue: TaskQueue[IO]): FlowOpF ~> FlowStateA =
      ioFlowInterpreter(taskQueue)(contextShift, timer)

    override def effectInterpreter: EffectF ~> FlowStateA = ioEffectInterpreter

    override def unsafeRun(io: IO[Unit]): Unit = {
      io.start.flatMap { fiber =>
        installHook(fiber).map(_ => fiber)
      }.flatMap(_.join).unsafeRunSync()
    }

    override def stop: IO[Unit] = IO(logger.info("shutdown")) >> IO(unsafeStop)

    def unsafeStop: Unit = {
      executorService.shutdownNow()
    }

    private def installHook(fiber: Fiber[IO, Unit]): IO[Unit] =
      IO {
        sys.addShutdownHook {
          // Should block the thread until all finalizers are executed
          fiber.cancel.unsafeRunSync()
        }
      }
  }

  object ParApp {
    val defaultConfig: ParConfig = ParConfig(
      schedulerConfig = SchedulerConfig(
        queueSize = 1000,
        numberOfWorkers = Runtime.getRuntime.availableProcessors(),
        workerQueueSize = 100,
        taskSubmissionTimeout = 5.seconds,
        workerTaskDequeueTimeout = 5.seconds,
        maxRedeliveryRetries = 5,
        redeliveryInitialDelay = 0.seconds))
  }

}