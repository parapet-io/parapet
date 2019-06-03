package io.parapet.core

import java.io.{PrintWriter, StringWriter}
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}

import cats.data.{EitherK, State}
import cats.effect.IO._
import cats.effect._
import cats.effect.concurrent.Semaphore
import cats.free.Free
import cats.implicits._
import cats.{InjectK, Monad, ~>}
import com.typesafe.scalalogging.StrictLogging
import io.parapet.core.Logging._
import io.parapet.core.Scheduler._
import io.parapet.core.annotations.experimental

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
  type ReceiveF[F[_]] = PartialFunction[Event, FlowF[F, Unit]]
  type Interpreter[F[_]] = FlowOpOrEffect[F, ?] ~> FlowStateF[F, ?]

  val ParapetPrefix = "parapet"

  //  Utils


  // Exceptions
  class UnknownProcessException(message: String) extends RuntimeException(message)
  class EventDeliveryException(message: String = "", cause: Throwable = null) extends RuntimeException(message, cause)
  class EventRecoveryException(message: String = "", cause: Throwable = null) extends RuntimeException(message, cause)

  case class ProcessRef(private[core] val ref: String) {
    override def toString: String = ref
  }
  object ProcessRef {
    val SystemRef = ProcessRef(ParapetPrefix + "-system")
    val DeadLetterRef = ProcessRef(ParapetPrefix + "-deadletter")
    val UnknownRef = ProcessRef(ParapetPrefix + "-unknown")
    def jdkUUIDRef: ProcessRef = new ProcessRef(UUID.randomUUID().toString)
  }

  trait Lock[F[_]] {
    def acquire: F[Unit]
    def release: F[Unit]
    def tryAcquire(time: FiniteDuration): F[Boolean]
  }

  object Lock {
    def apply[F[_] : Concurrent: Timer]: F[Lock[F]] = Semaphore(1).map { s =>
      new Lock[F] {
        override def acquire: F[Unit] = s.acquire

        override def release: F[Unit] = s.release

        override def tryAcquire(time: FiniteDuration): F[Boolean] = {
          Concurrent[F].race(s.acquire, Timer[F].sleep(time)).flatMap {
            case Left(_) => Concurrent[F].pure(true)
            case Right(_) => Concurrent[F].pure(false)
          }
        }
      }
    }
  }

  trait Event {
    final val id: String = UUID.randomUUID().toString
  }
  case object Start extends Event
  case object Stop extends Event
  // event should be not lazy
  case class Envelope(sender: ProcessRef, event: () => Event, receiver: ProcessRef) extends Event
  case class Failure(pRef: ProcessRef, event: Event, error: Throwable) extends Event
  case class DeadLetter(failure: Failure) extends Event

  implicit class EventOps[F[_]](e: => Event) {
    def ~>(process: ProcessRef)(implicit FL: Flow[F, FlowOpOrEffect[F, ?]]): FlowF[F, Unit] = FL.send(e, process)
    private[core] def ~>(process: Process[F])(implicit FL: Flow[F, FlowOpOrEffect[F, ?]]): FlowF[F, Unit] = FL.send(e, process.ref)

    @experimental
    def ~>>(process: ProcessRef)(implicit FL: Flow[F, FlowOpOrEffect[F, ?]]): FlowF[F, Unit] = FL.sendEager(e, process)
    private[core] def ~>>(process: Process[F])(implicit FL: Flow[F, FlowOpOrEffect[F, ?]]): FlowF[F, Unit] = FL.sendEager(e, process.ref)
  }

  trait Queue[F[_], A] {
    def enqueue(e: => A): F[Unit] // todo don't need to be lazy
    def enqueueAll(es: Seq[A])(implicit M:Monad[F]): F[Unit] = es.map(e => enqueue(e)).foldLeft(M.unit)(_ >> _)
    def dequeue: F[A]
    def tryDequeue: F[Option[A]]
    def dequeueThrough[F1[x] >: F[x] : Monad, B](f: A => F1[B]): F1[B] = {
      implicitly[Monad[F1]].flatMap(dequeue)(a => f(a))
    }

    // returns a tuple where Element 2 contains elements that match the given predicate
    def partition(p: A => Boolean)(implicit M: Monad[F]): F[(Seq[A], Seq[A])] = {
      def partition(left: Seq[A], right: Seq[A]): F[(Seq[A], Seq[A])] = {
        tryDequeue.flatMap {
          case Some(a) => if (p(a)) partition(left, right :+ a) else partition(left :+ a, right)
          case None => M.pure((left, right))
        }
      }

      partition(ListBuffer(), ListBuffer())
    }
  }

  object Queue {
    def bounded[F[_] : Concurrent, A](capacity: Int): F[Queue[F, A]] = {
      for {
        q <- fs2.concurrent.Queue.bounded[F, A](capacity)
      } yield new Queue[F, A] {
        override def enqueue(e: => A): F[Unit] = q.enqueue1(e)

        override def dequeue: F[A] = q.dequeue1

        override def tryDequeue: F[Option[A]] = q.tryDequeue1
      }
    }

    def unbounded[F[_] : Concurrent, A]: F[Queue[F, A]] = {
      for {
        q <- fs2.concurrent.Queue.unbounded[F, A]
      } yield new Queue[F, A] {
        override def enqueue(e: => A): F[Unit] = q.enqueue1(e)

        override def dequeue: F[A] = q.dequeue1

        override def tryDequeue: F[Option[A]] = q.tryDequeue1
      }
    }
  }

  trait QueueModule[F[_], A] {
    def queue: Queue[F, A]
  }


  trait Parallel[F[_]] {
    // runs given effects in parallel and returns a single effect
    def par(effects: Seq[F[_]]): F[Unit]
  }

  object Parallel {
    def apply[F[_] : Parallel]: Parallel[F] = implicitly[Parallel[F]]
  }

  implicit def ioParallel(implicit ctx: ContextShift[IO]): Parallel[IO] = (effects: Seq[IO[_]]) => effects.toList.parSequence_

  implicit class EffectOps[F[_] : Concurrent : Timer, A](fa: F[A]) {
    def retryWithBackoff(initialDelay: FiniteDuration, maxRetries: Int, backoffBase: Int = 2): F[A] =
      EffectOps.retryWithBackoff(fa, initialDelay, maxRetries, backoffBase)
  }

  object EffectOps {
    def retryWithBackoff[F[_], A](fa: F[A],
                                  initialDelay: FiniteDuration,
                                  maxRetries: Int, backoffBase: Int = 2)
                                 (implicit timer: Timer[F], ce: Concurrent[F]): F[A] = {
      fa.handleErrorWith { error =>
        if (maxRetries > 0)
          timer.sleep(initialDelay) >> retryWithBackoff(fa, initialDelay * backoffBase, maxRetries - 1)
        else
          ce.raiseError(error)
      }
    }
  }

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
  case class Send[F[_]](f: () => Event, receivers: Seq[ProcessRef]) extends FlowOp[F, Unit]
  case class EagerSend[F[_]](e: Event, receivers: Seq[ProcessRef]) extends FlowOp[F, Unit]
  case class Par[F[_], G[_]](flow: Seq[Free[G, Unit]]) extends FlowOp[F, Unit]
  case class Delay[F[_], G[_]](duration: FiniteDuration, flow: Option[Free[G, Unit]]) extends FlowOp[F, Unit]
  case class Stop[F[_]]() extends FlowOp[F, Unit]
  case class Reply[F[_], G[_]](f: ProcessRef => Free[G, Unit]) extends FlowOp[F, Unit]

  // F - effect type
  // G - target program
  class Flow[F[_], G[_]](implicit I: InjectK[FlowOp[F, ?], G]) {
    val empty: Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Empty())
    val terminate: Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Stop())

    def use[A](resource: => A)(flow: A => Free[G, Unit]): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Use(() => resource, flow))

    // sends event `e` to the list of receivers
    def send(e: => Event, receiver: ProcessRef, other: ProcessRef*): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Send(() => e, receiver +: other))

    @experimental
    def sendEager(e: Event, receiver: ProcessRef, other: ProcessRef*): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](EagerSend(e, receiver +: other))

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

  implicit class FreeOps[F[_], A](fa: Free[F, A]) {
    // alias for Free flatMap
    def ++[B](fb: Free[F, B]): Free[F, B] = fa.flatMap(_ => fb)
  }

  // Interpreters for ADTs based on cats IO
  type IOFlowOpOrEffect[A] = FlowOpOrEffect[IO, A]

  def ioFlowInterpreter(taskQueue: TaskQueue[IO])(implicit ctx: ContextShift[IO], timer: Timer[IO]): FlowOp[IO, ?] ~> FlowStateF[IO, ?] = new (FlowOp[IO, ?] ~> FlowStateF[IO, ?]) {

    override def apply[A](fa: FlowOp[IO, A]): FlowStateF[IO, A] = {

      val parallel: Parallel[IO] = ioParallel
      val interpreter: Interpreter[IO] = ioFlowInterpreter(taskQueue) or ioEffectInterpreter
      fa match {
        case Empty() => State[FlowState[IO], Unit] {s => (s, ())}
        case Use(resource, flow) => State[FlowState[IO], Unit] { s =>
          val res = IO.delay(resource()) >>= (r => interpret(flow(r).asInstanceOf[FlowF[IO, A]], interpreter, s.copy(ops = ListBuffer())).toList.sequence)
          (s.addOps(Seq(res)), ())
        }
        case Stop() => State[FlowState[IO], Unit] { s => (s.addOps(Seq(taskQueue.enqueue(Terminate()))), ()) }
        case Send(thunk, receivers) =>
          State[FlowState[IO], Unit] { s =>
            val ops = receivers.map(receiver => taskQueue.enqueue(Deliver(Envelope(s.selfRef, thunk, receiver))))
            (s.addOps(ops), ())
          }
        case EagerSend(event, receivers) =>
          State[FlowState[IO], Unit] { s =>
            // todo use tryEnqueue and if it fails write event to disk
            val ops = receivers.map(receiver => taskQueue.enqueue(Deliver(Envelope(s.selfRef, () => event, receiver))))
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

  // <-------------- Process -------------->
  trait Process[F[_]] {
    self =>
    type ProcessFlow = FlowF[F, Unit]
    type Receive = ReceiveF[F]

    private[core] val flowOps = implicitly[Flow[F, FlowOpOrEffect[F, ?]]]
    private[core] val effectOps = implicitly[Effects[F, FlowOpOrEffect[F, ?]]]

    val name: String = "default"

    val ref: ProcessRef = ProcessRef.jdkUUIDRef

    val handle: Receive

    def apply(e: Event,
              ifUndefined: => ProcessFlow = implicitly[Flow[F, FlowOpOrEffect[F, ?]]].empty): ProcessFlow =
      if (handle.isDefinedAt(e)) handle(e)
      else ifUndefined

    // composition of this and `pb` process
    // todo add tests
    def ++(that: Process[F]): Process[F] = new Process[F] {
      override val handle: Receive = {
        case e => self.handle(e) ++ that.handle(e)
      }
    }

    override def toString: String = s"process[name=$name, ref=$ref]"
  }

  object Process {

    def apply[F[_]](receive: ProcessRef => PartialFunction[Event, FlowF[F, Unit]]): Process[F] = new Process[F] {
      override val handle: Receive = receive(this.ref)
    }

    def named[F[_]](pName: String, receive: ProcessRef => PartialFunction[Event, FlowF[F, Unit]]): Process[F] = new Process[F] {
      override val name: String = pName
      override val handle: Receive = receive(this.ref)
    }

  }

  // <-------------- System processes -------------->

  class SystemProcess[F[_]] extends Process[F] {
    override val name: String = SystemRef.ref
    override val ref: ProcessRef = SystemRef
    override val handle: Receive = {
      case f: Failure => flowOps.send(DeadLetter(f), DeadLetterRef)
    }
  }

  trait DeadLetterProcess[F[_]] extends Process[F] {
    override val name: String = DeadLetterRef.ref
    override final val ref: ProcessRef = DeadLetterRef
  }

  object DeadLetterProcess {

    class DeadLetterLoggingProcess[F[_]](implicit ce: Concurrent[F]) extends DeadLetterProcess[F] {
      override val name: String = DeadLetterRef.ref + "-logging"
      override val handle: Receive = {
        case DeadLetter(Failure(pRef, event, error)) =>
          val errorMsg = error match {
            case e: EventDeliveryException => e.getCause.getMessage
            case e: EventRecoveryException => e.getCause.getMessage
          }

          val mdcFields: MDCFields = Map(
            "processId" -> ref,
            "processName" -> name,
            "failedProcessId" -> pRef,
            "eventId" -> event.id,
            "errorMsg" -> errorMsg,
            "stack_trace" -> getStackTrace(error))

          effectOps.eval {
            logger.mdc(mdcFields) { args =>
              logger.debug(s"$name: process[id=$pRef] failed to process event[id=${event.id}], error=${args("errorMsg")}")
            }
          }
      }


      private def getStackTrace(throwable: Throwable): String = {
        val sw = new StringWriter
        val pw = new PrintWriter(sw, true)
        throwable.printStackTrace(pw)
        sw.getBuffer.toString
      }
    }

    def logging[F[_] : Concurrent]: DeadLetterProcess[F] = new DeadLetterLoggingProcess()

  }

  trait CatsProcess extends Process[IO]

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
    private[core] val systemProcess: Process[F] = new SystemProcess[F]

    def flowInterpreter(taskQueue: TaskQueue[F]): FlowOpF ~> FlowStateA

    def effectInterpreter: EffectF ~> FlowStateA

    val program: ProcessFlow

    def unsafeRun(f: F[Unit]): Unit

    def stop: F[Unit]

    private[core] final def initProcesses(implicit F: Flow[F, FlowOpOrEffect[F, ?]]): Free[FlowOpOrEffect[F, ?], Unit] =
      processes.map(p => Start ~> p).foldLeft(F.empty)(_ ++ _)

    def run: F[AppContext[F]] = {
      if (processes.isEmpty) {
        concurrentEffect.raiseError(new RuntimeException("Initialization error:  at least one process must be provided"))
      } else {
        val systemProcesses = Array(systemProcess, deadLetter)
        for {
          taskQueue <- Queue.bounded[F, Task[F]](config.schedulerConfig.queueCapacity)
          context <- concurrentEffect.pure(AppContext(taskQueue))
          interpreter <- concurrentEffect.pure(flowInterpreter(taskQueue) or effectInterpreter)
          scheduler <- concurrentEffect.pure(new Scheduler[F](taskQueue, config.schedulerConfig,
            systemProcesses ++ processes, interpreter))
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
    override val parallel: Parallel[IO] = implicitly[Parallel[IO]]
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
        numberOfWorkers = Runtime.getRuntime.availableProcessors(),
        queueCapacity = 1000,
        workerQueueCapacity = 100,
        taskSubmissionTimeout = 60.seconds,
        maxRedeliveryRetries = 5,
        redeliveryInitialDelay = 0.seconds))
  }

}