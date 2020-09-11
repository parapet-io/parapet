package io.parapet.core

import java.util.concurrent.ConcurrentHashMap

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


 sometimes lock released with value  == 0, no interference, no  notification
 but there are more messages waiting in the queue
 example:  immediately terminates process and delivers Stop event
 use release only for the case when pq.queue is Ø
 otherwise always use  release2

 */
trait Scheduler[F[_]] {
  def start: F[Unit]

  def submit(task: Task[F]): F[SubmissionResult]
}

object Scheduler {

  case class Signal(ref: ProcessRef, trace: Trace) {
    override def toString: String = s"Signal(ref=$ref, trace=${trace.value})"
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

  // todo temporary solution
  case class LoggerWrapper[F[_] : Concurrent](logger: Logger, stdio: Boolean = false) {
    private val ct = Concurrent[F]

    def debug(msg: => String): F[Unit] = {
      if (stdio) ct.delay(println(msg))
      else ct.delay(logger.debug(msg))
    }

    def error(msg: => String): F[Unit] = {
      if (stdio) ct.delay(println(msg))
      else ct.delay(logger.error(msg))
    }

    def error(msg: => String, cause: Throwable): F[Unit] = {
      if (stdio) ct.delay {
        println(msg)
        cause.printStackTrace()
      }
      else ct.delay(logger.error(msg, cause))
    }

    def info(msg: => String): F[Unit] = {
      if (stdio) ct.delay(println(msg))
      else ct.delay(logger.error(msg))
    }

    def warn(msg: String): F[Unit] = {
      if (stdio) ct.delay(println(msg))
      else ct.delay(logger.warn(msg))
    }
  }


  // todo: revisit
  sealed trait SubmissionResult

  object Ok extends SubmissionResult

  object UnknownProcess extends SubmissionResult

  object TerminatedProcess extends SubmissionResult

  object ProcessQueueIsFull extends SubmissionResult

  import SchedulerImpl._

  class SchedulerImpl[F[_] : Concurrent : ParAsync : Parallel : ContextShift](
                                                                               config: SchedulerConfig,
                                                                               context: Context[F],
                                                                               processRefQueue: Queue[F, Signal],
                                                                               interpreter: Interpreter[F]) extends Scheduler[F] {

    private val ct = Concurrent[F]
    private val pa = implicitly[Parallel[F]]
    private val logger = LoggerWrapper(Logger(LoggerFactory.getLogger(getClass.getCanonicalName)))

    override def start: F[Unit] = {
      ct.bracket(ct.delay(createWorkers)) { workers =>
        pa.par(workers.map(w => w.run))
      } { _ =>
        stopProcess(ProcessRef.SystemRef,
          context, ProcessRef.SystemRef, interpreter,
          (pRef, err) => logger.error(s"An error occurred while stopping process $pRef", err)) >>
          logger.info("scheduler has been shut down")
      }
    }

    /**
      * Puts the given task into process internal queue.
      *
      * @param ps   process state
      * @param task task to submit
      * @return {{{Ok}}} if the task has been added, {{{ProcessQueueIsFull}}} if the process internal queue is full
      */
    private def submit(ps: ProcessState[F], task: Deliver[F]): F[SubmissionResult] = {
      ps.tryPut(task) >>= {
        case true =>
          logger.debug(s"Scheduler::submit(ps=${ps.process}, task=$task) - task added to the process queue") >>
            ps.acquired.flatMap {
              case true => logger.debug(s"Scheduler::submit(ps=${ps.process}, task=$task) - lock is already acquired. don't notify")
              case false =>
                for {
                  sig <- ct.pure(Signal(ps.process.ref, Trace().append(s"Scheduler::submit(ps=${ps.process}, task=$task)")))
                  _ <- processRefQueue.enqueue(sig)
                  _ <- logger.debug(s"Scheduler::submit(ps=${ps.process}, task=$task) - added to notification queue. trace_id=${sig.trace.id}")
                } yield ()


            } >> ct.pure(Ok)
        case false =>
          send(ProcessRef.SystemRef, Failure(task.envelope,
            EventDeliveryException(s"System failed to deliver an event to process ${ps.process}",
              EventQueueIsFullException(s"process ${ps.process} event queue is full"))),
            task.envelope.sender, interpreter) >> ct.pure(ProcessQueueIsFull)
      }
    }

    def sendUnknownProcessError(e: Envelope): F[Unit] = {
      send(
        SystemRef,
        Failure(e, UnknownProcessException(s"there is no such process with id=${e.receiver} registered in the system")),
        e.sender,
        interpreter)
    }

    def sendTerminatedProcessError(e: Envelope): F[Unit] = {
      send(
        SystemRef,
        Failure(e, TerminatedProcessException(s"process  ${e.receiver} was terminated via Stop or Kill")),
        e.sender,
        interpreter)
    }

    override def submit(task: Task[F]): F[SubmissionResult] = {
      task match {
        case deliverTask@Deliver(e@Envelope(sender, event, pRef)) =>
          ct.suspend {
            context.getProcessState(pRef)
              .fold[F[SubmissionResult]](sendUnknownProcessError(e).map(_ => UnknownProcess)) { ps =>
                event match {
                  case Kill =>
                    // interruption is a concurrent operation
                    // i.e. interrupt may be completed but
                    // the actual process may be still performing some computations
                    // we need to submit Stop event here instead of `direct call`
                    // to avoid race condition between interruption and process stop
                    context.interrupt(pRef).flatMap(a => logger.debug(s"Scheduler::submit(ps=$ps, task=$task) - interrupted = $a")) >>
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

    val pLockHistory: java.util.Map[ProcessRef, Trace] = new ConcurrentHashMap() // todo tmp solution for troubleshooting

    class Worker[F[_] : Concurrent : ParAsync : Parallel : ContextShift](name: String,
                                                                         context: Context[F],
                                                                         processRefQueue: Queue[F, Signal],
                                                                         interpreter: Interpreter[F]) {
      private val logger = LoggerWrapper(Logger(LoggerFactory.getLogger(s"parapet-$name")))
      private val ct = implicitly[Concurrent[F]]

      def run: F[Unit] = {
        def step: F[Unit] = {
          logger.debug(s"worker[$name] waiting on processRefQueue") >>
            processRefQueue.dequeue >>= { signal =>
            val thisTrace = signal.trace.append(s"worker[$name] got signal")
            context.getProcessState(signal.ref) match {
              case Some(ps) =>
                ps.acquire >>= {
                  case true =>
                    ct.delay(pLockHistory.put(signal.ref, thisTrace)) >>
                      run(ps, thisTrace.append(s"worker[$name] acquired lock")) >> step
                  case false =>
                    logger.debug(thisTrace.append(
                      s"worker[$name] failed to acquire lock; " +
                        s"last time acquired by trace_id = ${Option(pLockHistory.get(signal.ref)).map(_.id).getOrElse("null")})"
                    ).value) >> step
                }

              case None =>
                logger.error(s"worker[$name] no such process $signal") >>
                  step // process was terminated and removed from the system,
              // eventually scheduler will stop delivering new events for this process
            }

          }
        }

        step
      }

      private def run(ps: ProcessState[F], trace: Trace): F[Unit] = {
        val nextTrace = trace.append(s"worker[$name]::run(${ps.process})")
        logger.debug(nextTrace.value) >>
          ps.tryTakeTask >>= {
          case Some(t: Deliver[F]) => deliver(ps, t.envelope, nextTrace)
          case Some(t) => ct.raiseError(new RuntimeException(s"unsupported task: $t"))
          case None => releaseWithOptNotify(ps, nextTrace)
        }
      }

      def releaseWithOptNotify(ps: ProcessState[F], trace: Trace): F[Unit] = {
        val thisTrace = trace.append(s"releaseWithOptNotify(${ps.process})")
        logger.debug(thisTrace.value) >>
          ps.release.flatMap {
            case true => logger.debug(thisTrace.append("released; no messages").value)
            case false =>
              for {
                sig <- ct.pure(Signal(ps.process.ref, thisTrace.append("released; more messages; added to notification queue")))
                _ <- processRefQueue.enqueue(sig)

              } yield ()
          }
      }

      def releaseAndNotify(ps: ProcessState[F], trace: Trace): F[Unit] = {
        val thisTrace = trace.append(s"releaseAndNotify(ps=${ps.process})")
        logger.debug(thisTrace.value) >>
          ps.release.flatMap {
            _ =>
              for {
                sig <- ct.pure(Signal(ps.process.ref, thisTrace.append("released; added to notification queue")))
                _ <- processRefQueue.enqueue(sig)
              } yield ()

          }
      }

      private def deliver(ps: ProcessState[F], envelope: Envelope, trace: Trace): F[Unit] = {
        val process = ps.process
        val event = envelope.event
        val sender = envelope.sender
        val receiver = envelope.receiver
        val thisTrace = trace.append(s"deliver(ps:${ps.process}, envelope=$envelope)")

        logger.debug(thisTrace.value) >>
          (event match {
            case Stop =>
              ps.stop().flatMap {
                case true =>
                  stopProcess(sender, context, process.ref, interpreter,
                    (_, err) => handleError(process, envelope, err)) >> context.remove(process.ref).void >>
                    releaseWithOptNotify(ps, thisTrace.append("stop_process")) // do we need to notify ?
                case false => sendToDeadLetter(
                  DeadLetter(envelope, new IllegalStateException(s"process=$process is already stopped")), interpreter) // >>
                // releaseWithOptNotify("ps.stop()->false", ps) // do we need to notify ?
                // if notify then switch behaviour and dynamic process creation fails
              }
            case _ =>
              ps.interrupted.product(ps.stopped).flatMap {
                case (_, true) | (true, _) =>
                  sendToDeadLetter(
                    DeadLetter(envelope, new IllegalStateException(s"process=$process is terminated")), interpreter) >>
                    releaseAndNotify(ps, thisTrace.append("terminated"))
                case _ =>
                  if (process.canHandle(event)) {
                    for {
                      flow <- ct.delay(process(event))
                      effect <- ct.pure(interpret_(flow, interpreter, FlowState[F](senderRef = sender, selfRef = receiver)))
                      _ <- runEffect(effect, envelope, ps, err => handleError(process, envelope, err), thisTrace.append("canHandle"))
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
                      case _ => logger.warn(errorMsg)
                    }
                    logMsg >> whenUndefined >> releaseAndNotify(ps, thisTrace.append("cannot handle"))
                  }
              }

          })
      }

      def runEffect(effect: F[Unit],
                    envelope: Envelope,
                    ps: ProcessState[F],
                    errorHandler: Throwable => F[Unit], trace: Trace): F[Unit] = {
        val thisTrace = trace.append(s"runEffect(ps=${ps.process}, envelope=$envelope)")
        logger.debug(thisTrace.value) >> runAsync(effect, envelope, ps, errorHandler, thisTrace)
      }


      def runAsync(flow: F[Unit],
                   envelope: Envelope,
                   ps: ProcessState[F],
                   errorHandler: Throwable => F[Unit], trace: Trace): F[Unit] = {
        val parAsync = implicitly[ParAsync[F]]


        val res = for {
          fiber <- ct.start(flow)
          _ <- parAsync.runAsync[Either[Unit, Unit]](ct.race(ps.interruption.get, fiber.join), {
            case Left(th) => errorHandler(th) >> releaseAndNotify(ps, trace.append(s"error=${th.getMessage}"))
            case Right(Left(_)) =>
              fiber.cancel >> releaseAndNotify(ps, trace.append("canceled"))
            case Right(Right(_)) => releaseAndNotify(ps, trace.append("completed"))
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