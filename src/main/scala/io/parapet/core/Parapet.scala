package io.parapet.core

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutorService, Executors}

import cats.data.{EitherK, State}
import cats.effect.IO._
import cats.effect._
import cats.effect.concurrent.Deferred
import cats.free.Free
import cats.implicits._
import cats.{InjectK, Monad, ~>}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.language.{higherKinds, implicitConversions, reflectiveCalls}
//import fs2.concurrent.{Queue => CQueue}
//
//import scala.collection.mutable.{Queue => SQueue}

// todo define failure scenarios
// todo Send failed message to dead letter queue
// todo add Lock ?
// todo integration tests
// todo use logger instead of println
// todo implement par(flow1, flow2, ...)
// todo how to reply, bi-directional
// todo resource release on shutdown
//
object Parapet {

  type <:<[F[_], G[_], A] = EitherK[F, G, A]
  type FlowOpOrEffect[F[_], A] = <:<[FlowOp[F, ?], Effect[F, ?], A]
  type FlowF[F[_], A] = Free[FlowOpOrEffect[F, ?], A]
  type FlowStateF[F[_], A] = State[FlowState[F], A]

  val SystemRef = ProcessRef("π.system")
  val UnknownRef = ProcessRef("π.unknown")

  case class ProcessRef(private[core] val ref: String)

  case class FlowState[F[_]](senderRef: ProcessRef, selfRef: ProcessRef, ops: Seq[F[_]]) {
    def addOps(that: Seq[F[_]]): FlowState[F] = this.copy(ops = this.ops ++ that)
  }

  class Interpreter[F[_]](interpreter: FlowOpOrEffect[F, ?] ~> FlowStateF[F, ?]) {
    def interpret[A](senderRef: ProcessRef, selfRef: ProcessRef, program: FlowF[F, A]): Seq[F[_]] =
      program.foldMap[FlowStateF[F, ?]](interpreter)
        .runS(FlowState(senderRef, selfRef, ListBuffer())).value.ops
  }

  trait Event

  case class Mail(sender: ProcessRef, event: () => Event) extends Event

  case object Start extends Event

  case object Stop extends Event

  case class Failure(error: Throwable, event: Event) extends Event

  implicit class EventOps[F[_]](e: => Event) {
    def ~>(process: ProcessRef)(implicit FL: Flow[F, FlowOpOrEffect[F, ?]]): FlowF[F, Unit] = FL.send(e, process) //?
    private[core] def ~>(process: Process[F])(implicit FL: Flow[F, FlowOpOrEffect[F, ?]]): FlowF[F, Unit] = FL.send(e, process.selfRef)
  }

  trait Queue[F[_], A] {
    def enqueue(e: => A): F[Unit]

    def dequeue: F[A]
  }

  object Queue {
    def bounded[F[_] : Concurrent, A](capacity: Int): F[Queue[F, A]] = {
      fs2.concurrent.Queue.bounded[F, A](capacity).map(q => new Queue[F, A] {
        override def enqueue(e: => A): F[Unit] =
          q.enqueue1(e) //*> implicitly[Concurrent[F]].delay(println("submitted task"))

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

  implicit def ioParallel(implicit ctx: ContextShift[IO]): Parallel[IO] = (effects: Seq[IO[_]]) => effects.toList.parSequence_

  implicit class EffectOps[F[_] : ConcurrentEffect : Timer, A](fa: F[A]) {
    def retryWithBackoff(initialDelay: FiniteDuration, maxRetries: Int, backoffBase: Int = 2): F[A] =
      EffectOps.retryWithBackoff(fa, initialDelay, maxRetries, backoffBase)
  }

  object EffectOps {
    def retryWithBackoff[F[_] : ConcurrentEffect : Timer, A](fa: F[A],
                                                             initialDelay: FiniteDuration,
                                                             maxRetries: Int, backoffBase: Int = 2): F[A] = {
      fa.handleErrorWith { error =>
        if (maxRetries > 0)
          implicitly[Timer[F]].sleep(initialDelay) *> implicitly[ConcurrentEffect[F]].delay(println(s"retry: $maxRetries")) *> retryWithBackoff(fa, initialDelay * backoffBase, maxRetries - 1)
        else
          implicitly[ConcurrentEffect[F]].raiseError(error)
      }
    }
  }

  // <----------- Schedule ----------->
  sealed trait Task[F[_]]

  case class Deliver[F[_]](event: () => Event, processRef: ProcessRef) extends Task[F] // todo consider change thunk to Mail
  case class Terminate[F[_]]() extends Task[F]

  type TaskQueue[F[_]] = F[Queue[F, Task[F]]]

  class Scheduler[F[_] : ConcurrentEffect : Timer : Parallel](
                                                               queue: Queue[F, Task[F]],
                                                               config: SchedulerConfig,
                                                               processes: Array[Process[F]],
                                                               interpreter: Interpreter[F],
                                                               cancelationSignal: Deferred[F, Unit],
                                                               completionSignal: Deferred[F, Unit]) {


    private val ce = implicitly[ConcurrentEffect[F]]
    private val parallel = implicitly[Parallel[F]]
    val processesMap: Map[String, Process[F]] = processes.map(p => p.name -> p).toMap

    def submit(task: Task[F]): F[Unit] = queue.enqueue(task)

    //def stop: F[Unit] = cancelationSignal.complete(()) *> completionSignal.get

    def awaitWorkers(workers: Seq[Worker[F]]): F[Unit] = {
      ce.delay(println("awaitWorkers")) *> workers.map(_.await).toList.sequence_
    }

    def cancelWorkers(workers: Seq[Worker[F]]): F[Unit] = {
      //println(workers)
      parallel.par(workers.map(_.cancel) :+ awaitWorkers(workers))
    }

    def reader(workers: Map[String, Worker[F]]): F[Unit] = {
      println("READER")
      workers.foreach {
        case (p, w) => println(s"process $p assigned to ${w.name}")
      }

      def step: F[Unit] = for {
        // _  <-  ConcurrentEffect[F].delay(println("reader is waiting for tasks"))
        task <- queue.dequeue
        _ <- task match {
          case Terminate() =>
            ce.delay(println("Scheduler - normal termination")) *>
             workers.values.map(_.queue.enqueue(task)).toList.sequence_ *> //  todo submit in par
//              awaitWorkers(workers.values.toSeq) *>
              ConcurrentEffect[F].delay(println("kill Scheduler"))
          case Deliver(_, processRef) => workers.get(processRef.ref)
            .fold(ConcurrentEffect[F].delay(println(s"unknown process: ${processRef.ref}. ignore event"))) {
              worker => worker.queue.enqueue(task)
            } *> step
        }
      } yield ()

      step *> ConcurrentEffect[F].delay(println("kill reader"))
    }

    def run: F[Unit] = {
      val window = Math.ceil(processes.length.toDouble / config.numberOfWorkers).toInt
      println(s"window: $window")
      val workersF: F[List[(Worker[F], Array[Process[F]])]] = processes.sliding(window, window).zipWithIndex.map {
        case (pgroup, i) =>
          val w: F[(Worker[F], Array[Process[F]])] =
            for {
              q <- Queue.bounded[F, Task[F]](config.numberOfWorkers)
              completionSignal <- Deferred[F, Unit]
            } yield (new Worker(
              s"worker-$i", q, pgroup.map(p => p.selfRef.ref ->p).toMap, config, interpreter,
              completionSignal), pgroup)

          w
      }.toList.sequence

      // val tmp: F[List[(Worker[F], Array[Process[F]])]] = workers.sequence

      //      val m: Map[Int, Int] = List((1, List(11, 12, 13)), (2, List(21, 22, 23))).flatMap {
      //        case (w, pg) => pg.map(p => (p ->  w))
      //      }.toMap

      ce.bracket(workersF) { workers =>
        parallel.par(workers.map(w => w._1.run *> ConcurrentEffect[F].delay(println("worker done"))) :+
          reader(workers.flatMap {
            case (w, pgroup) => pgroup.map(_.name -> w)
          }.toMap) *> ConcurrentEffect[F].delay(println("reader done"))) *>
          ConcurrentEffect[F].delay(println("scheduler done"))
      } { workers =>
        ce.delay(println(s"cancel workers: $workers")) *> cancelWorkers(workers.map(_._1))
      }

      //      (for {
      //        workers <- workersF
      //        _ <- implicitly[Parallel[F]].par(workers.map(_._1.run *> ConcurrentEffect[F].delay(println("worker done"))) :+
      //          reader(workers.flatMap {
      //          case (w, pgroup) => pgroup.map(_.name -> w)
      //        }.toMap)*> ConcurrentEffect[F].delay(println("reader done")))
      //
      //
      //      } yield ()) *>  ConcurrentEffect[F].delay(println("scheduler done"))

      // implicitly[Parallel[F]].par((0 until numOfWorkers).map(i => new Worker(s"worker-$i", queue, interpreter).run))
    }
  }

  trait SchedulerModule[F[_]] {
    val scheduler: Scheduler[F]
  }

  def interpretAndRun[F[_] : Monad, A](program: FlowF[F, A], senderRef: ProcessRef, selfRef: ProcessRef, interpreter: Interpreter[F]): F[Unit] =
    interpreter.interpret(senderRef, selfRef, program).fold(Monad[F].unit)(_ *> _).void

  // implicitly[Interpreter[F]].interpret(program).toList.sequence_

  class Worker[F[_] : ConcurrentEffect : Timer : Parallel](
                                                            val name: String,
                                                            val queue: Queue[F, Task[F]],
                                                            processesMap: Map[String, Process[F]],
                                                            config: SchedulerConfig,
                                                            interpreter: Interpreter[F],
                                                            completionSignal: Deferred[F, Unit]) {
    private val ce = implicitly[ConcurrentEffect[F]]
    private val parallel = implicitly[Parallel[F]]

    def processTask(task: Task[F]): F[Unit] = {
      task match {
        case Deliver(eventThunk, processRef) =>
          val (sender, event) = eventThunk().asInstanceOf[Mail] match {
            case m@Mail(_, _) => (m.sender, m.event())
          }

          val process: Parapet.Process[F] = processesMap(processRef.ref)
          val program: FlowF[F, Unit] = process.handle.apply(event) // todo check if defined
          val res = interpretAndRun(program, sender, processRef, interpreter)
          res.retryWithBackoff(config.redeliveryInitialDelay, config.maxRedeliveryRetries).handleErrorWith { err =>
            val failure = Failure(err, event)
            if (process.handle.isDefinedAt(failure)) {
              interpretAndRun(process.handle.apply(failure), SystemRef, processRef, interpreter).handleErrorWith { err =>
                ce.delay(println(s"failed to process failure event. source event = $event, error = $err"))
              }
            } else {
              ce.delay(println("process error handling is not defined"))
            }
          }
        case _ => ce.raiseError(new RuntimeException("unsupported task"))
      }
    }

    def deliverStopAndNotify: F[Unit] = {
      parallel.par(processesMap.values.map { p =>
        if (p.handle.isDefinedAt(Stop)) {
          ce.delay(println(s"$name delivers Stop to ${p.selfRef.ref}")) *> interpretAndRun(p.handle.apply(Stop), SystemRef, p.selfRef, interpreter)
        } else {
          ce.unit //  add debug
        }
      }.toSeq) *> completionSignal.complete(())
    }

    def run: F[Unit] = {
      def step: F[Unit] =
        for {
          task <- queue.dequeue
          _ <- task match {
            case Deliver(_, _) => /*implicitly[Monad[F]].pure(println(s"$name dequeued task: " + task)) *>*/ processTask(task) *> step
            case Terminate() => ce.delay(println(s"normal termination $name"))
          }
        } yield ()
      step
    }

    def cancel: F[Unit] = ce.delay(println(s"cancel worker $name")) *>  deliverStopAndNotify
    def await: F[Unit] = completionSignal.get
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
    val terminate: Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Stop())

    // sends event `e` to the list of receivers
    def send(e: => Event, receiver: ProcessRef, other: ProcessRef*): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Send(() => e, receiver +: other))

    // changes sequential execution to parallel
    def par(flow: Free[G, Unit], other: Free[G, Unit]*): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Par(flow +: other))

    // delays execution of the given `flow`
    def delay(duration: FiniteDuration, flow: Free[G, Unit]): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Delay(duration, Some(flow)))

    def delay(duration: FiniteDuration): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](Delay(duration, None))

    def reply(f: ProcessRef => Free[G, Unit]): Free[G, Unit] = Free.inject[FlowOp[F, ?], G](SenderOp(f))
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

  def ioFlowInterpreter[Env <: TaskQueueModule[IO] with CatsModule](env: Env): FlowOp[IO, ?] ~> FlowStateF[IO, ?] = new (FlowOp[IO, ?] ~> FlowStateF[IO, ?]) {

    // todo replace with interpreter
    def run[A](flow: FlowF[IO, A], senderRef: ProcessRef, selfRef: ProcessRef, interpreter: IOFlowOpOrEffect ~> FlowStateF[IO, ?]): Seq[IO[_]] = {
      flow.foldMap(interpreter).runS(FlowState(senderRef, selfRef, ListBuffer())).value.ops
    }

    override def apply[A](fa: FlowOp[IO, A]): FlowStateF[IO, A] = {
      implicit val ctx: ContextShift[IO] = env.ctx
      implicit val ioTimer: Timer[IO] = env.timer
      val parallel: Parallel[IO] = ioParallel
      val interpreter: IOFlowOpOrEffect ~> FlowStateF[IO, ?] = ioFlowInterpreter(env) or ioEffectInterpreter
      fa match {
        case Empty() => State.set(FlowState[IO](UnknownRef, UnknownRef, ListBuffer.empty))
        case Stop() => State[FlowState[IO], Unit] { s => (s.addOps(Seq(env.taskQueue.enqueue(Terminate()))), ()) }
        case Send(thunk, receivers) =>
          State[FlowState[IO], Unit] { s =>
            val ops = receivers.map(receiver => env.taskQueue.enqueue(Deliver(() => Mail(s.selfRef, thunk), receiver)))
            (s.addOps(ops), ())
          }
        case Par(flows) =>
          State[FlowState[IO], Unit] { s =>
            val res = parallel.par(
              flows.map(flow => interpretAndRun(flow.asInstanceOf[FlowF[IO, A]], s.senderRef, s.selfRef, new Interpreter[IO](interpreter))))
            (s.addOps(Seq(res)), ())
          }
        case Delay(duration, Some(flow)) =>
          State[FlowState[IO], Unit] { s =>
            val delayIO = IO.sleep(duration)
            val res = run(flow.asInstanceOf[FlowF[IO, A]], s.senderRef, s.selfRef, interpreter).map(op => delayIO *> op)
            (s.addOps(res), ())
          }
        case Delay(duration, None) =>
          State[FlowState[IO], Unit] { s => (s.addOps(Seq(IO.sleep(duration))), ()) }

        case SenderOp(f) =>
          State[FlowState[IO], Unit] { s =>
            (
              s.addOps(run(f(s.senderRef).asInstanceOf[FlowF[IO, A]], s.senderRef, s.selfRef, interpreter)),
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

    def selfRef: ProcessRef = ProcessRef(name)

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
    //    def apply(): IO[CatsAppEnv] = {
    //      implicit val ctx: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
    //      for {
    //        taskQueue <- Queue.bounded[IO, Task[IO]](10)
    //      } yield CatsAppEnv(taskQueue, ctx, IO.timer(ExecutionContext.global))
    //    }
  }

  case class SchedulerConfig(numberOfWorkers: Int,
                             maxRedeliveryRetries: Int,
                             redeliveryInitialDelay: FiniteDuration)

  case class ParConfig(schedulerConfig: SchedulerConfig)

  abstract class ParApp[F[+ _], Env <: TaskQueueModule[F]] {
    type EffectF[A] = Effect[F, A]
    type FlowOpF[A] = FlowOp[F, A]
    type FlowStateA[A] = FlowStateF[F, A]
    type ProcessFlow = FlowF[F, Unit]

    val config: ParConfig = ParApp.config
    val environment: F[Env]
    implicit val parallel: Parallel[F]
    implicit val timer: Timer[F]
    implicit val concurrentEffect: ConcurrentEffect[F]
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
          schedulerCancelationSignal <- Deferred[F, Unit]
          schedulerCompletionSignal <- Deferred[F, Unit]
          interpreter <- concurrentEffect.pure(new Interpreter[F](flowInterpreter(env) or effectInterpreter(env)))
          scheduler <- concurrentEffect.pure(new Scheduler[F](env.taskQueue, config.schedulerConfig,
            processes, interpreter, schedulerCancelationSignal, schedulerCompletionSignal))
          _ <- parallel.par(Seq(interpretAndRun(program, SystemRef, SystemRef, interpreter) *>
            concurrentEffect.delay(println("program finished")),
            scheduler.run
            //concurrentEffect.guarantee(scheduler.run)(scheduler.stop)
          ))
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

    override def stop: IO[Unit] = IO(println("shutdown")) *> IO(unsafeStop)

    def unsafeStop: Unit = {
      println("executorService.shutdownNow()")
      println(executorService.shutdownNow())
      println("done")
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
    val config: ParConfig = ParConfig(
      schedulerConfig = SchedulerConfig(
        numberOfWorkers = Runtime.getRuntime.availableProcessors(),
        maxRedeliveryRetries = 5,
        redeliveryInitialDelay = 0.seconds
      )
    )
  }

}

import io.parapet.core.Parapet.CatsApp

object MyApp extends CatsApp {

  import CounterProcess._
  import io.parapet.core.Parapet._
  import io.parapet.core.catsInstances.effect._
  import io.parapet.core.catsInstances.flow._ // for Effect DSL

  val counter: io.parapet.core.Parapet.Process[IO] = new CounterProcess()

  override val processes: Array[io.parapet.core.Parapet.Process[IO]] = Array(counter)

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

  override val program: ProcessFlow = {
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
  import io.parapet.core.Parapet._
  import io.parapet.core.catsInstances.effect._
  import io.parapet.core.catsInstances.flow._

  import scala.concurrent.duration._

  val counter1: Process[IO] = new CounterProcess("counter1")
  val counter2: Process[IO] = new CounterProcess("counter2")

  override val processes: Array[io.parapet.core.Parapet.Process[IO]] = Array(counter1, counter2)

  class CounterProcess(id: String) extends CatsProcess {
    val counter = new AtomicInteger()
    override val name: String = id
    override val handle: Receive = {
      case Inc => delay(1.second, eval {
        counter.incrementAndGet()
      }) ++ eval(println(s"counter1=${counter.get()}"))
      case Print => suspend(IO.delay(println(s"counter2=${counter.get()}")))
      case Stop => eval(println(s"s$id -- stop"))
    }
  }

  object CounterProcess {

    object Inc extends Event

    object Print extends Event

  }

  override val program: CounterApp.ProcessFlow = {
    def create(p: Process[IO]): CounterApp.ProcessFlow = {
      eval(println(s"send Inc message to ${p.name}")) ++
        Inc ~> p ++
        eval(println(s"send Print message to ${p.name}")) ++
        Print ~> p
    }

    par(delay(2.seconds) ++ create(counter1), create(counter2)) ++ terminate
    // create(counter1) ++ create(counter2) ++ terminate
  }
}