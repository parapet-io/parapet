package io.parapet.core

import java.util.UUID
import java.util.concurrent.{ExecutorService, Executors}

import cats.data.{EitherK, State}
import cats.effect.IO._
import cats.effect._
import cats.free.Free
import cats.implicits._
import cats.{InjectK, Monad, ~>}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.language.{higherKinds, implicitConversions, reflectiveCalls}

object Parapet extends StrictLogging {
  import ProcessRef._

  type <:<[F[_], G[_], A] = EitherK[F, G, A]
  type FlowOpOrEffect[F[_], A] = <:<[FlowOp[F, ?], Effect[F, ?], A]
  type FlowF[F[_], A] = Free[FlowOpOrEffect[F, ?], A]
  type FlowStateF[F[_], A] = State[FlowState[F], A]
  type Interpreter[F[_]] = FlowOpOrEffect[F, ?] ~> FlowStateF[F, ?]

  case class ProcessRef(private[core] val ref: String) {
    override def toString: String = ref
  }
  object ProcessRef {
    val SystemRef = ProcessRef("π.system")
    val UnknownRef = ProcessRef("π.unknown")
  }

  case class FlowState[F[_]](senderRef: ProcessRef, selfRef: ProcessRef, ops: Seq[F[_]]) {
    def addOps(that: Seq[F[_]]): FlowState[F] = this.copy(ops = ops ++ that)
  }

  object FlowState {
    def apply[F[_]](senderRef: ProcessRef, selfRef: ProcessRef): FlowState[F] =
      FlowState(senderRef = senderRef, selfRef = selfRef, ops = ListBuffer())
  }

  trait Event
  case object Start extends Event
  case object Stop extends Event
  case class Envelope(sender: ProcessRef, event: () => Event, receiver: ProcessRef) extends Event
  case class Failure(error: Throwable, event: Event) extends Event

  implicit class EventOps[F[_]](e: => Event) {
    def ~>(process: ProcessRef)(implicit FL: Flow[F, FlowOpOrEffect[F, ?]]): FlowF[F, Unit] = FL.send(e, process) //?
    private[core] def ~>(process: Process[F])(implicit FL: Flow[F, FlowOpOrEffect[F, ?]]): FlowF[F, Unit] = FL.send(e, process.ref)
  }

  trait Queue[F[_], A] {
    def enqueue(e: => A): F[Unit]
    def dequeue: F[A]
  }

  object Queue {
    def bounded[F[_] : Concurrent, A](capacity: Int): F[Queue[F, A]] = {
      fs2.concurrent.Queue.bounded[F, A](capacity).map(q => new Queue[F, A] {
        override def enqueue(e: => A): F[Unit] = q.enqueue1(e)
        override def dequeue: F[A] = q.dequeue1
      })
    }
  }

  trait QueueModule[F[_], A] {
    def queue: Queue[F, A]
  }

  type TaskQueue[F[_]] = Queue[F, Task[F]]

  trait TaskQueueModule[F[_]] {
    def taskQueue: TaskQueue[F]
  }

  trait Parallel[F[_]] {
    // runs given effects in parallel and returns a single effect
    def par(effects: Seq[F[_]]): F[Unit]
  }

  implicit def ioParallel(implicit ctx: ContextShift[IO]): Parallel[IO] = (effects: Seq[IO[_]]) => effects.toList.parSequence_

  implicit class EffectOps[F[_] : ConcurrentEffect : Timer, A](fa: F[A]) {
    def retryWithBackoff(initialDelay: FiniteDuration, maxRetries: Int, backoffBase: Int = 2): F[A] =
      EffectOps.retryWithBackoff(fa, initialDelay, maxRetries, backoffBase)
  }

  object EffectOps {
    def retryWithBackoff[F[_], A](fa: F[A],
                                  initialDelay: FiniteDuration,
                                  maxRetries: Int, backoffBase: Int = 2)
                                 (implicit timer: Timer[F], ce: ConcurrentEffect[F]): F[A] = {
      fa.handleErrorWith { error =>
        if (maxRetries > 0)
          timer.sleep(initialDelay) >> retryWithBackoff(fa, initialDelay * backoffBase, maxRetries - 1)
        else
          ce.raiseError(error)
      }
    }
  }

  // <----------- Schedule ----------->
  sealed trait Task[F[_]]
  case class Deliver[F[_]](envelope: Envelope) extends Task[F]
  case class Terminate[F[_]]() extends Task[F]

  class Scheduler[F[_] : ConcurrentEffect : Timer : Parallel: ContextShift](
                                                               queue: TaskQueue[F],
                                                               config: SchedulerConfig,
                                                               processes: Array[Process[F]],
                                                               interpreter: Interpreter[F]) {

    private val ce = implicitly[ConcurrentEffect[F]]
    private val parallel = implicitly[Parallel[F]]
    private val ctxShift = implicitly[ContextShift[F]]

    def submit(task: Task[F]): F[Unit] = queue.enqueue(task)

    def stopWorkers(workers: Seq[Worker[F]]): F[Unit] = {
      ce.delay(logger.debug("Scheduler - stop workers")) >> parallel.par(workers.map(_.stop))
    }

    def consumer(workers: Map[ProcessRef, Worker[F]]): F[Unit] = {
      def step: F[Unit] =
        queue.dequeue.flatMap {
          case task@Terminate() =>
            ce.delay(logger.debug("Scheduler - normal termination")) >>
              workers.values.map(_.queue.enqueue(task)).toList.sequence_
          case task@Deliver(Envelope(_, _, receiver)) => workers.get(receiver)
            .fold(ce.delay(logger.warn(s"unknown process: $receiver. Ignore event"))) {
              worker => worker.queue.enqueue(task)
            } >> step
        }

      step
    }

    def run: F[Unit] = {
      val window = Math.ceil(processes.length.toDouble / config.numberOfWorkers.toDouble).toInt
      val groupedProcesses: List[(Array[Process[F]], Int)] = processes.sliding(window, window).zipWithIndex.toList

      val workersF: F[List[Worker[F]]] = groupedProcesses.map {
        case (processGroup, i) =>
          Queue.bounded[F, Task[F]](config.workerQueueCapacity).map { workerQueue =>
            new Worker(
              s"π-worker-$i",
              workerQueue,
              processGroup,
              config,
              interpreter)
          }

      }.sequence

      ce.bracket(workersF) { workers =>
        val processToWorker: Map[ProcessRef, Worker[F]] = workers.flatMap(worker => worker.processes.map(_.ref -> worker)).toMap
        parallel.par(workers.map(worker => ctxShift.shift >> worker.run) :+ consumer(processToWorker))
      }(stopWorkers)

    }
  }

  trait SchedulerModule[F[_]] {
    val scheduler: Scheduler[F]
  }

  private[core] def interpret[F[_] : Monad, A](program: FlowF[F, A], interpreter: Interpreter[F], state: FlowState[F]): Seq[F[_]] = {
    program.foldMap[FlowStateF[F, ?]](interpreter).runS(state).value.ops
  }

  private[core] def interpret_[F[_] : Monad, A](program: FlowF[F, A], interpreter: Interpreter[F], state: FlowState[F]): F[Unit] = {
    interpret(program, interpreter, state).fold(Monad[F].unit)(_ >> _).void
  }

  class Worker[F[_] : ConcurrentEffect : Timer : Parallel](
                                                            val name: String,
                                                            val queue: TaskQueue[F],
                                                            val processes: Seq[Process[F]],
                                                            config: SchedulerConfig,
                                                            interpreter: Interpreter[F]) {
    private val ce = implicitly[ConcurrentEffect[F]]
    private val parallel = implicitly[Parallel[F]]
    private val processesMap: Map[ProcessRef, Process[F]] = processes.map(p => p.ref -> p).toMap

    def deliver(envelope: Envelope): F[Unit] = {
      val event = envelope.event()
      val sender = envelope.sender
      val receiver = envelope.receiver
      processesMap.get(receiver)
        .fold(ce.raiseError[Unit](new RuntimeException(s"unknown process: $receiver"))) { process =>
          if (process.handle.isDefinedAt(event)) {
            val program = process.handle.apply(event)
            interpret_(program, interpreter, FlowState(senderRef = sender, selfRef = receiver))
              .retryWithBackoff(config.redeliveryInitialDelay, config.maxRedeliveryRetries)
              .handleErrorWith { err =>
                val failure = Failure(err, event)
                if (process.handle.isDefinedAt(failure)) {
                  interpret_(process.handle.apply(failure), interpreter, FlowState(senderRef = SystemRef, selfRef = receiver))
                    .handleErrorWith { err =>
                      ce.delay(logger.warn(s"$receiver failed to process Failure event. Source event = $event, error = $err"))
                    }
                } else ce.delay(logger.warn(s"failure event case isn't defined in $receiver"))
              }
          } else ce.unit // todo add config property: failOnUnmatched
        }
    }

    def processTask(task: Task[F]): F[Unit] = {
      task match {
        case Deliver(envelope) => deliver(envelope)
        case unknown => ce.raiseError(new RuntimeException(s"$name - unsupported task: $unknown"))
      }
    }

    def run: F[Unit] = {
      def step: F[Unit] =
        queue.dequeue.flatMap {
          case task@Deliver(_) => processTask(task) >> step
          case Terminate() => ce.delay(logger.debug(s"$name - normal termination"))
        }

      step
    }

    def deliverStopEvent: F[Unit] = {
      parallel.par(processes.map { process =>
        if (process.handle.isDefinedAt(Stop)) {
          ce.delay(logger.debug(s"$name - deliver Stop to ${process.ref}")) >>
            interpret_(process.handle.apply(Stop), interpreter, FlowState(senderRef = SystemRef, selfRef = process.ref))
        } else {
          ce.unit // todo add config property: failOnUnmatched
        }
      })
    }

    def stop: F[Unit] = ce.delay(logger.debug(s"$name - stop")) >> deliverStopEvent
  }

  // <----------- Flow ADT ----------->
  sealed trait FlowOp[F[_], A]
  case class Empty[F[_]]() extends FlowOp[F, Unit]
  case class Send[F[_]](f: () => Event, receivers: Seq[ProcessRef]) extends FlowOp[F, Unit]
  case class Par[F[_], G[_]](flow: Seq[Free[G, Unit]]) extends FlowOp[F, Unit]
  case class Delay[F[_], G[_]](duration: FiniteDuration, flow: Option[Free[G, Unit]]) extends FlowOp[F, Unit]
  case class Stop[F[_]]() extends FlowOp[F, Unit]
  case class SenderOp[F[_], G[_]](f: ProcessRef => Free[G, Unit]) extends FlowOp[F, Unit]

  // F - effect type
  // G - target program
  class Flow[F[_], G[_]](implicit I: InjectK[FlowOp[F, ?], G]) {
    val empty: Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Empty())
    val ø: Free[G, Unit] = empty
    val terminate: Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Stop())

    // sends event `e` to the list of receivers
    def send(e: => Event, receiver: ProcessRef, other: ProcessRef*): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Send(() => e, receiver +: other))

    // executes operations from the given flow in parallel
    def par(flow: Free[G, Unit], other: Free[G, Unit]*): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Par(flow +: other))

    // delays execution of each operation in the given flow
    // i.e. delay(duration, x~>p ++ y~>p) <-> delay(duration, x~>p) ++ delay(duration, y~>p) <-> delay(duration) ++ x~>p ++ delay(duration) ++ y~>p
    def delay(duration: FiniteDuration, flow: Free[G, Unit]): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Delay(duration, Some(flow)))

    // adds delays to the current flow, delays execution of any subsequent operation
    def delay(duration: FiniteDuration): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Delay(duration, None))

    def reply(f: ProcessRef => Free[G, Unit]): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](SenderOp(f))
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

  implicit class FreeOps[F[_], A](fa: Free[F, A]) {
    // alias for Free flatMap
    def ++[B](fb: Free[F, B]): Free[F, B] = fa.flatMap(_ => fb)
  }

  // Interpreters for ADTs based on cats IO
  type IOFlowOpOrEffect[A] = FlowOpOrEffect[IO, A]

  def ioFlowInterpreter[Env <: TaskQueueModule[IO] with CatsModule](env: Env): FlowOp[IO, ?] ~> FlowStateF[IO, ?] = new (FlowOp[IO, ?] ~> FlowStateF[IO, ?]) {

    override def apply[A](fa: FlowOp[IO, A]): FlowStateF[IO, A] = {
      implicit val ctx: ContextShift[IO] = env.ctx
      implicit val ioTimer: Timer[IO] = env.timer
      val parallel: Parallel[IO] = ioParallel
      val interpreter: Interpreter[IO] = ioFlowInterpreter(env) or ioEffectInterpreter
      fa match {
        case Empty() => State.set(FlowState[IO](UnknownRef, UnknownRef, ListBuffer.empty))
        case Stop() => State[FlowState[IO], Unit] { s => (s.addOps(Seq(env.taskQueue.enqueue(Terminate()))), ()) }
        case Send(thunk, receivers) =>
          State[FlowState[IO], Unit] { s =>
            val ops = receivers.map(receiver => env.taskQueue.enqueue(Deliver(Envelope(s.selfRef, thunk, receiver))))
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

        case SenderOp(f) =>
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

  // <-------------- Process -------------->
  trait Process[F[_]] {
    self =>
    type ProcessFlow = FlowF[F, Unit] //  replaced  with FlowF[F]
    type Receive = PartialFunction[Event, ProcessFlow]

    val name: String = UUID.randomUUID().toString // todo revisit

    def ref: ProcessRef = ProcessRef(name)

    val handle: Receive

    // composition of this and `pb` process
    // todo add tests
    def ++(that: Process[F]): Process[F] = new Process[F] {
      override val handle: Receive = {
        case e => self.handle(e) ++ that.handle(e)
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

  case class CatsAppEnv(taskQueue: TaskQueue[IO],
                        ctx: ContextShift[IO],
                        timer: Timer[IO]) extends TaskQueueModule[IO] with CatsModule

  case class SchedulerConfig(numberOfWorkers: Int,
                             queueCapacity: Int,
                             workerQueueCapacity: Int,
                             maxRedeliveryRetries: Int,
                             redeliveryInitialDelay: FiniteDuration)

  case class ParConfig(schedulerConfig: SchedulerConfig)

  abstract class ParApp[F[+ _], Env <: TaskQueueModule[F]] {
    type EffectF[A] = Effect[F, A]
    type FlowOpF[A] = FlowOp[F, A]
    type FlowStateA[A] = FlowStateF[F, A]
    type ProcessFlow = FlowF[F, Unit]

    val config: ParConfig = ParApp.defaultConfig
    val environment: F[Env]
    implicit val parallel: Parallel[F]
    implicit val timer: Timer[F]
    implicit val concurrentEffect: ConcurrentEffect[F]
    implicit val contextShift: ContextShift[F]
    val processes: Array[Process[F]]

    def flowInterpreter(e: Env): FlowOpF ~> FlowStateA

    def effectInterpreter(e: Env): EffectF ~> FlowStateA

    val program: ProcessFlow

    def unsafeRun(f: F[Unit]): Unit

    def stop: F[Unit]

    def run: F[Env] = {
      if (processes.isEmpty) {
        concurrentEffect.raiseError(new RuntimeException("Initialization error:  at least one process must be provided"))
      } else {
        for {
          env <- environment
          interpreter <- concurrentEffect.pure(flowInterpreter(env) or effectInterpreter(env))
          scheduler <- concurrentEffect.pure(new Scheduler[F](env.taskQueue, config.schedulerConfig,
            processes, interpreter))
          _ <- parallel.par(Seq(interpret_(program, interpreter, FlowState(SystemRef, SystemRef)), scheduler.run))
          _ <- stop
        } yield env
      }

    }

    def main(args: Array[String]): Unit = {
      unsafeRun(run.void)
    }
  }

  abstract class CatsApp extends ParApp[IO, CatsAppEnv] {
    val executorService: ExecutorService = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors())
    implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(executorService)
    implicit val contextShift: ContextShift[IO] = IO.contextShift(executionContext)
    override val parallel: Parallel[IO] = implicitly[Parallel[IO]]
    override val timer: Timer[IO] = IO.timer(executionContext)
    override val concurrentEffect: ConcurrentEffect[IO] = implicitly[ConcurrentEffect[IO]]

    override val environment: IO[CatsAppEnv] = for {
      taskQueue <- Queue.bounded[IO, Task[IO]](10)
    } yield CatsAppEnv(taskQueue, contextShift, timer)

    override def flowInterpreter(e: CatsAppEnv): FlowOpF ~> FlowStateA = ioFlowInterpreter(e)

    override def effectInterpreter(e: CatsAppEnv): EffectF ~> FlowStateA = ioEffectInterpreter

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
        numberOfWorkers = Runtime.getRuntime.availableProcessors(),
        queueCapacity = 100,
        workerQueueCapacity = 100,
        maxRedeliveryRetries = 5,
        redeliveryInitialDelay = 0.seconds))
  }

}