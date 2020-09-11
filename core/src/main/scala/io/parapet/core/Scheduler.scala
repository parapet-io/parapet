package io.parapet.core

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import cats.effect.{Concurrent, ContextShift}
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.semigroupal._
import com.typesafe.scalalogging.Logger
import io.parapet.core.Context.ProcessState
import io.parapet.core.Dsl.{Dsl, FlowOps}
import io.parapet.core.DslInterpreter._
import io.parapet.core.Event._
import io.parapet.core.ProcessRef._
import io.parapet.core.Queue.ChannelType
import io.parapet.core.Scheduler._
import io.parapet.core.exceptions._
import org.slf4j.LoggerFactory

/*
  NQ - notification queue
  PQ - process queue
  flow:
  (1) Scheduler.submit(deliver)
    (1.1) add to PQ
    (1.2) check a process's lock
      - if lock is already taken
           increment lock count and return true
        else
           return false
    if (1.1) false, add pref to NQ
  (2) Worker dequeues pref from NQ
    (2.1) acquire a process lock
    (2.2) if (2.1) = true
            asynchronously process event
          else
            go to (2)



 lock states: Ø lock is not taken. 0...n lock is taken
 syntax:
 {action} -> result |  result, {new_state}

 NQ - notification queue
 e - event
 ex - execution
 p - process
 w - worker
 e1 ~> p1 - Scheduler.submit(e1, p1)

 Invariants:
 1.)
 // given
 processes: [p1]
 workers: [w1, w2]
 NQ - empty
 p1.lock = Ø

 // executions
 e1 ~> p1
    p1.queue.add(e1) -> true, {p1.queue=[e1]}
    p1.lock.acquired -> false
    NQ.add(p1) -> (), {PQ=[p1]}
 w1 removes p1 from NQ -> p1, {NQ=[]}
   w1 p1.lock.acquire -> true, {p1.lock=0}
   w1 removes e1 from p1.queue -> e1, {p1.queue=[]}
   w1 submitted task to p1 for execution -> exe1
 e2 ~> p1
    p1.queue.add(e2) -> true, {p1.queue=[e2]}
    p1.lock.acquired -> true, {p1.lock=1}
 (w1 | w2) removes p1 from PQ -> p1, {PQ=[]}
    w1 p1.lock.acquire -> false, {p1.lock=1}
 exe1 completed
    p1.lock.release -> lock_count=1, {p1.lock=Ø}
    if lock_count > 0
       NQ.add(p1) -> (), {PQ=[p1]}


 */
trait Scheduler[F[_]] {
  def start: F[Unit]

  def submit(task: Task[F]): F[SubmissionResult]
}

object Scheduler {

  case class Signal(ref: ProcessRef, info: String, ts: Long = System.nanoTime()) {

  }

  sealed trait Task[F[_]]

  case class Deliver[F[_]](envelope: Envelope) extends Task[F]

  type TaskQueue[F[_]] = Queue[F, Task[F]]

  def apply[F[_] : Concurrent : ParAsync : Parallel : ContextShift](config: SchedulerConfig,
                                                                    context: Context[F],
                                                                    interpreter: Interpreter[F]): F[Scheduler[F]] = {
    SchedulerImpl(config, context, interpreter)
  }

  case class SchedulerConfig(numberOfWorkers: Int) {
    require(numberOfWorkers > 0)
  }

  object SchedulerConfig {
    val default: SchedulerConfig = SchedulerConfig(
      numberOfWorkers = 2)
  }

  // todo: revisit
  sealed trait SubmissionResult

  object Ok extends SubmissionResult

  object UnknownProcess extends SubmissionResult

  object ProcessQueueIsFull extends SubmissionResult

  import SchedulerImpl._

  class SchedulerImpl[F[_] : Concurrent : ParAsync : Parallel : ContextShift](
                                                                               config: SchedulerConfig,
                                                                               context: Context[F],
                                                                               processRefQueue: Queue[F, Signal],
                                                                               interpreter: Interpreter[F]) extends Scheduler[F] {


    private val ct = Concurrent[F]
    private val pa = implicitly[Parallel[F]]
    private val logger = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))

    override def start: F[Unit] = {
      ct.bracket(ct.delay(createWorkers)) { workers =>
        pa.par(workers.map(w => w.run))
      } { _ =>
        stopProcess(ProcessRef.SystemRef,
          context, ProcessRef.SystemRef, interpreter,
          (pRef, err) => ct.delay(logger.error(s"An error occurred while stopping process $pRef", err))) >>
          ct.delay(logger.info("scheduler has been shut down"))
      }
    }


    private def submit(ps: ProcessState[F], task: Deliver[F]): F[SubmissionResult] = {

      ps.tryPut(task) >>= {
        case true =>
          log(ps, s"Scheduler::submit ${msg(ps, task.envelope)} submitted") >>
            ps.acquired.flatMap {
              //case true => ct.delay(println(s"Scheduler::submit ${msg(ps, task.envelope)} is already acquired, don't notify"))
              case _ =>
                for {
                  sig <- ct.pure(Signal(ps.process.ref, "Scheduler:submit"))
                  _ <- processRefQueue.enqueue(sig)
                  _ <- log(ps, task, s"Scheduler::submit ${msg(ps, task.envelope)} added to notification queue $sig")
                } yield ()


            } >> ct.pure(Ok)
        case false =>
          send(ProcessRef.SystemRef, Failure(task.envelope,
            EventDeliveryException(s"System failed to deliver an event to process ${ps.process}",
              EventQueueIsFullException(s"process ${ps.process} event queue is full"))),
            task.envelope.sender, interpreter) >> ct.pure(ProcessQueueIsFull)
      }
    }

    override def submit(task: Task[F]): F[SubmissionResult] = {
      task match {
        case deliverTask@Deliver(e@Envelope(sender, event, pRef)) =>
          ct.suspend {
            context.getProcessState(pRef)
              .fold[F[SubmissionResult]](send(
                SystemRef,
                Failure(e, UnknownProcessException(s"there is no such process with id=$pRef registered in the system")),
                sender,
                interpreter) >> ct.pure(UnknownProcess)) { ps =>
                event match {
                  case Kill =>
                    // interruption is a concurrent operation
                    // i.e. interrupt may be completed but
                    // the actual process may be still performing some computations
                    // we need to submit Stop event here instead of `direct call`
                    // to avoid race condition between interruption and process stop
                    context.interrupt(pRef).flatMap(r => ct.delay(println(s"${msg(ps, e)} interrupted = $r"))) >>
                      submit(ps, Deliver(Envelope(sender, Stop, pRef)))
                  case _ => submit(ps, deliverTask)
                }
              }
          }
        case t => ct.raiseError(new RuntimeException(s"unsupported task: $t"))
      }
    }

    private def createWorkers: List[Worker[F]] = {
      (1 to config.numberOfWorkers).map { i => {
        new Worker[F](s"worker-$i", context, processRefQueue, interpreter)
      }
      }.toList
    }


  }


  object SchedulerImpl {


    //  debug helpers
    def msg[F[_]](p: ProcessState[F], envelope: Envelope): String = {
      s"process [${p.process}], envelope: $envelope"
    }

    def apply[F[_] : Concurrent : ParAsync : Parallel : ContextShift](
                                                                       config: SchedulerConfig,
                                                                       context: Context[F],
                                                                       interpreter: Interpreter[F]): F[Scheduler[F]] =
      for {
        processRefQueue <- Queue.unbounded[F, Signal](ChannelType.MPMC)
      } yield
        new SchedulerImpl(
          config,
          context,
          processRefQueue,
          interpreter)

    val pLockHolder: java.util.Map[ProcessRef, String] = new ConcurrentHashMap()
    val logCount: AtomicInteger = new AtomicInteger()
    val systemProcesses = Set(ProcessRef.SystemRef, ProcessRef.DeadLetterRef)

    def shouldLog[F[_]](ps: ProcessState[F]): Boolean = !systemProcesses.contains(ps.process.ref)

    def log[F[_] : Concurrent](ps: ProcessState[F], task: Deliver[F], m: => String): F[Unit] = {
      val ct = implicitly[Concurrent[F]]
      if (shouldLog(ps)) log(m)
      else ct.unit
    }

    def log[F[_] : Concurrent](ps: ProcessState[F], m: => String): F[Unit] = {
      val ct = implicitly[Concurrent[F]]
      if (shouldLog(ps)) log(m)
      else ct.unit
    }

    def log[F[_] : Concurrent](msg: String): F[Unit] = {
      val ct = implicitly[Concurrent[F]]
      ct.delay(println(System.nanoTime() + "-" + logCount.incrementAndGet() + "=>" + msg))
    }

    class Worker[F[_] : Concurrent : ParAsync : Parallel : ContextShift](name: String,
                                                                         context: Context[F],
                                                                         processRefQueue: Queue[F, Signal],
                                                                         interpreter: Interpreter[F]) {

      private val logger = Logger(LoggerFactory.getLogger(s"parapet-$name"))
      private val ct = implicitly[Concurrent[F]]

      def run: F[Unit] = {
        def step: F[Unit] = {
          // ct.delay(println("worker step")) >>
          log(s"worker[$name] waiting on processRefQueue") >>
            processRefQueue.dequeue >>= { signal =>
            log(s"worker[$name] got signal: $signal") >>
              (context.getProcessState(signal.ref) match {
                case Some(ps) =>
                  ps.acquire >>= {
                    case true => log(ps, s"worker[$name] acquired lock ${ps.process}") >>
                      ct.delay(pLockHolder.put(signal.ref, name)) >> run(ps) >> step
                    case false =>
                      log(ps, s"worker[$name] failed to acquire lock ${ps.process}. lock held by ${pLockHolder.get(signal.ref)}") >> step
                  }

                case None =>
                  ct.delay(println(s"worker[$name] no such process ${signal.ref}")) >>
                    step // process was terminated and removed from the system,
                // eventually scheduler will stop delivering new events for this process
              })

          }
        }

        step
      }

      private def run(ps: ProcessState[F]): F[Unit] = {
        ps.tryTakeTask >>= {
          case Some(t: Deliver[F]) => log(ps, t, s"worker[$name] delivers ${msg(ps, t.envelope)}") >>
            deliver(ps, t.envelope) >> log(ps, t, s"worker[$name] delivered ${msg(ps, t.envelope)}")

          case Some(t) => ct.raiseError(new RuntimeException(s"unsupported task: $t"))
          case None => releaseProcess("run(ps: ProcessState[F])", ps)
        }
      }

      def releaseProcess(msg: String, ps: ProcessState[F]): F[Unit] = {
        ps.release.flatMap {
          case true => log(ps, s"$msg:: ${ps.process} released. no messages")
          case false =>
            for {
              sig <- ct.pure(Signal(ps.process.ref, msg + ".releaseProcess2"))
              _ <- processRefQueue.enqueue(sig)
              _ <- log(ps, s"$msg:: ${ps.process.ref} released. more messages. added to notification queue $sig")
            } yield ()

        }
      }

      def releaseProcess2(msg: String, ps: ProcessState[F]): F[Unit] = {
        ps.release.flatMap {
          _ =>
            for {
              sig <- ct.pure(Signal(ps.process.ref, msg + ".releaseProcess2"))
              _ <- processRefQueue.enqueue(sig)
              _ <- log(ps, s"$msg.releaseProcess2::---${ps.process.ref} released. added to notification queue $sig")
            } yield ()

        }
      }

      private def deliver(ps: ProcessState[F], envelope: Envelope): F[Unit] = {
        val process = ps.process
        val event = envelope.event
        val sender = envelope.sender
        val receiver = envelope.receiver


        event match {

          case Stop =>
            // move it to Scheduler.submit
            ps.stop().flatMap {
              case true => ct.delay(println("stopProcess")) >> stopProcess(sender, context, process.ref, interpreter,
                (_, err) => handleError(process, envelope, err)) >> context.remove(process.ref).void >>
                releaseProcess("ps.stop()", ps)
              case false => sendToDeadLetter(
                DeadLetter(envelope, new IllegalStateException(s"process: $process is already stopped")), interpreter)
            }
          case _ =>
            // move it to Scheduler.submit
            ps.interrupted.product(ps.stopped).flatMap {
              case (_, true) | (true, _) =>
                ct.delay(println(s"${msg(ps, envelope)} terminated !!!")) >>
                  sendToDeadLetter(
                    DeadLetter(envelope, new IllegalStateException(s"process: $process is terminated")), interpreter)
              case _ =>
                if (process.canHandle(event)) {
                  for {
                    _ <- ct.delay(println(s"${msg(ps, envelope)} can handle"))
                    flow <- ct.delay(process(event))
                    effect <- ct.pure(interpret_(flow, interpreter, FlowState[F](senderRef = sender, selfRef = receiver)))
                    _ <- runEffect(effect, envelope, ps, err => handleError(process, envelope, err))
                  } yield ()

                } else {

                  val errorMsg = s"process $process handler is not defined for event: $event"
                  val whenUndefined = event match {
                    case f: Failure =>
                      // no error handling, send to dead letter
                      sendToDeadLetter(DeadLetter(f), interpreter)
                    case Start => ct.unit // ignore lifecycle events
                    case _ =>
                      send(ProcessRef.SystemRef,
                        Failure(envelope, EventMatchException(errorMsg)), envelope.sender, interpreter)
                    // sendToDeadLetter(DeadLetter(envelope, EventMatchException(errorMsg)), interpreter)
                  }
                  val logMsg = event match {
                    case Start | Stop => ct.unit
                    case _ => ct.delay(logger.warn(errorMsg))
                  }
                  logMsg >> whenUndefined >> releaseProcess(s"${ps.process}.deliver.no_match on $event", ps)
                }
            }

        }
      }

      def runEffect(effect: F[Unit],
                    envelope: Envelope,
                    ps: ProcessState[F],
                    errorHandler: Throwable => F[Unit]): F[Unit] = {
        ct.delay(println(s"${msg(ps, envelope)} runs effect")) >>
          runBlocking(effect, envelope, ps, errorHandler) >>
          ct.delay(println(s"${msg(ps, envelope)} effect started async"))
      }


      def runBlocking(flow: F[Unit],
                      envelope: Envelope,
                      ps: ProcessState[F],
                      errorHandler: Throwable => F[Unit]): F[Unit] = {
        val parAsync = implicitly[ParAsync[F]]

        val release: F[Unit] = releaseProcess2(s"runBlocking[$envelope]", ps)

        val res = for {
          fiber <- ct.start(flow)
          _ <- parAsync.runAsync[Either[Unit, Unit]](ct.race(ps.interruption.get, fiber.join), {
            case Left(th) => errorHandler(th) >> release
            case Right(Left(_)) => ct.delay(println(s"${msg(ps, envelope)} was canceled]")) >>
              fiber.cancel >> ps.stop() >> deliverStopEvent(envelope.sender, ps.process, interpreter) >>  release
            case Right(Right(_)) => ct.delay(println(s"${msg(ps, envelope)} completed")) >> release
          })
        } yield ()

        res



        //        for {
        //          fiber <- ct.start(flow)
        //          _ <- ct.start(run(fiber, ps, errorHandler)) // run and forget
        //        } yield ()
      }

      //      def run(fiber: Fiber[F, Unit],
      //              ps: ProcessState[F],
      //              errorHandler: Throwable => F[Unit]): F[Unit] = {
      //        ct.race(fiber.join, ps.interruption.get).flatMap {
      //          case Left(_) => ct.unit // note: wont be executed if `fiber.join` failed
      //          case Right(_) => fiber.cancel // process was interrupted, cancel current flow
      //        }.guarantee(ps.release >> processRefQueue.enqueue(ps.process.ref))
      //          .handleErrorWith(errorHandler) // error handling should not be interrupted even if the process was interrupted
      //      }

      private def handleError(process: Process[F], envelope: Envelope, cause: Throwable): F[Unit] = {
        val event = envelope.event

        event match {
          case f: Failure =>
            ct.delay(logger.error(s"process $process has failed to handle Failure event. send to deadletter", cause)) >>
              sendToDeadLetter(DeadLetter(f), interpreter)
          case _ =>
            val errMsg = s"process $process has failed to handle event: $event"
            ct.delay(logger.error(errMsg, cause)) >>
              sendErrorToSender(envelope, EventHandlingException(errMsg, cause))
        }
      }

      private def sendErrorToSender(envelope: Envelope, err: Throwable): F[Unit] = {
        send(SystemRef, Failure(envelope, err), envelope.sender, interpreter)
      }

    }

    private def sendToDeadLetter[F[_] : Concurrent](dl: DeadLetter, interpreter: Interpreter[F])
                                                   (implicit flowDsl: FlowOps[F, Dsl[F, ?]]): F[Unit] = {
      send(SystemRef, dl, DeadLetterRef, interpreter)
    }

    private def send[F[_] : Concurrent](sender: ProcessRef,
                                        event: Event,
                                        receiver: ProcessRef,
                                        interpreter: Interpreter[F])(implicit flowDsl: FlowOps[F, Dsl[F, ?]]): F[Unit] = {
      interpret_(flowDsl.send(event, receiver), interpreter,
        FlowState[F](senderRef = sender, selfRef = receiver))
    }

    private def deliverStopEvent[F[_] : Concurrent](sender: ProcessRef,
                                                    process: Process[F],
                                                    interpreter: Interpreter[F]): F[Unit] = {
      val ct = implicitly[Concurrent[F]]
      if (process.canHandle(Stop)) {
        interpret_(
          process(Stop),
          interpreter,
          FlowState[F](senderRef = sender, selfRef = process.ref))
      } else {
        ct.unit
      }
    }

    private def stopProcess[F[_] : Concurrent : Parallel](sender: ProcessRef,
                                                          context: Context[F],
                                                          ref: ProcessRef,
                                                          interpreter: Interpreter[F],
                                                          onError: (ProcessRef, Throwable) => F[Unit]): F[Unit] = {
      val ct = implicitly[Concurrent[F]]
      val pa = implicitly[Parallel[F]]

      ct.suspend {
        val stopChildProcesses =
          pa.par(context.child(ref).map(child =>
            context.getProcessState(child).map {
              case ps if !ps.isStopped => stopProcess(ref, context, child, interpreter, onError)
              case _ => ct.unit
            }.getOrElse(ct.unit)))


        stopChildProcesses >>
          (context.getProcess(ref) match {
            case Some(p) => deliverStopEvent(sender, p, interpreter).handleErrorWith(err => onError(ref, err))
            case None => ct.unit // todo: revisit
          })
      }
    }

  }


}