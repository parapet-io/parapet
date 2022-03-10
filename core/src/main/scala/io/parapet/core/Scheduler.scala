package io.parapet.core

import cats.effect.{Concurrent, ContextShift, ExitCase, Timer}
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.semigroupal._
import com.typesafe.scalalogging.Logger
import io.parapet.ProcessRef._
import io.parapet.core.Context.ProcessState
import io.parapet.core.Dsl.{Dsl, FlowOps}
import io.parapet.core.DslInterpreter.Interpreter
import io.parapet.core.Events._
import io.parapet.core.Queue.ChannelType
import io.parapet.core.Scheduler._
import io.parapet.core.exceptions._
import io.parapet.{Envelope, Event, ProcessRef}
import org.slf4j.LoggerFactory

import scala.util.Try

trait Scheduler[F[_]] {
  def start: F[Unit]

  def submit(task: Task[F]): F[SubmissionResult]
}

object Scheduler {

  case object Inbox extends Event

  case class Signal(envelop: Envelope, execTrace: ExecutionTrace) {
    override def toString: String = s"Signal($envelop, $execTrace)"
  }

  sealed trait Task[F[_]]

  case class Deliver[F[_]](envelope: Envelope, execTrace: ExecutionTrace) extends Task[F]

  type TaskQueue[F[_]] = Queue[F, Task[F]]

  def apply[F[_]: Concurrent: Timer: Parallel: ContextShift](
      config: SchedulerConfig,
      context: Context[F],
      interpreter: Interpreter[F]
  ): F[Scheduler[F]] =
    SchedulerImpl(config, context, interpreter)

  case class SchedulerConfig(numberOfWorkers: Int) {
    require(numberOfWorkers > 0)
  }

  object SchedulerConfig {
    val default: SchedulerConfig = SchedulerConfig(numberOfWorkers = Runtime.getRuntime.availableProcessors())
  }

  case class LoggerWrapper[F[_]: Concurrent](logger: Logger, devMode: Boolean) {
    private val ct = Concurrent[F]

    def debug(msg: => String): F[Unit] =
      if (devMode) ct.delay(logger.debug(msg))
      else ct.unit

    def error(msg: => String): F[Unit] =
      if (devMode) ct.delay(logger.error(msg))
      else ct.unit

    def error(msg: => String, cause: Throwable): F[Unit] =
      if (devMode) ct.delay(logger.error(msg, cause))
      else ct.unit

    def info(msg: => String): F[Unit] =
      if (devMode) ct.delay(logger.info(msg))
      else ct.unit

    def warn(msg: => String): F[Unit] =
      if (devMode) ct.delay(logger.warn(msg))
      else ct.unit
  }

  // internal events
  private[core] case object NotifyEvent extends Event

  // todo: revisit
  sealed trait SubmissionResult
  case object Ok extends SubmissionResult
  case object UnknownProcess extends SubmissionResult
  case object TerminatedProcess extends SubmissionResult
  case object ProcessQueueIsFull extends SubmissionResult

  import SchedulerImpl._

  class SchedulerImpl[F[_]: Concurrent: Timer: Parallel: ContextShift](
      config: SchedulerConfig,
      context: Context[F],
      signalQueue: Queue[F, Signal],
      interpreter: Interpreter[F]
  ) extends Scheduler[F] {

    private val ct = Concurrent[F]
    private val pa = implicitly[Parallel[F]]
    private val logger = LoggerWrapper(Logger(LoggerFactory.getLogger(getClass.getCanonicalName)), context.devMode)

    override def start: F[Unit] =
      ct.bracketCase(ct.delay(createWorkers)) { workers =>
        pa.par(workers.map(w => w.run))
      } { (_, c) =>
        (c match {
          case ExitCase.Completed => logger.debug("all workers completed")
          case ExitCase.Error(e)  => logger.error("something went wrong", e)
          case ExitCase.Canceled  => logger.debug("scheduler has been canceled")
        }) >>
          stopProcess(
            ProcessRef.SystemRef,
            context,
            ProcessRef.SystemRef,
            interpreter,
            context.createTrace,
            logger,
            (pRef, err) => logger.error(s"An error occurred while stopping process $pRef", err)
          ) >> context.saveEventLog >> logger.info("scheduler has been shut down")
      }

    /** Adds the given task into a process internal event queue.
      * Optionally adds a signal to notification queue if the target process isn't being processed by a worker.
      *
      * @param ps   process state
      * @param task task to submit
      * @return {{{Ok}}} if the task has been added, {{{ProcessQueueIsFull}}} if the process internal queue is full
      */
    private def submit(ps: ProcessState[F], task: Deliver[F]): F[SubmissionResult] =
      ps.tryPut(task) >>= {
        case true =>
          logger.debug(s"Scheduler::submit(ps=${ps.process}, task=$task) - task added to the process queue") >>
            ps.acquired.flatMap {
              case true =>
                logger.debug(
                  s"Scheduler::submit(ps=${ps.process}, task=$task) - lock is already acquired. don't notify")
              case false =>
                for {
                  _ <- signalQueue.enqueue(Signal(task.envelope, task.execTrace))
                  _ <- logger.debug(
                    s"Scheduler::submit(ps=${ps.process}, task=$task) - added to notification queue. " +
                      s"traceId:${task.execTrace.last}")
                } yield ()

            } >> ct.pure(Ok)
        case false =>
          logger.warn(s"process ${ps.process} event queue is full") >>
          ct.pure(ProcessQueueIsFull)
      }

    def sendUnknownProcessError(task: Deliver[F]): F[Unit] = {
      val e = task.envelope
      send(
        SystemRef,
        Failure(e, UnknownProcessException(s"there is no such process with id=${e.receiver} registered in the system")),
        context.getProcessState(e.sender).get,
        interpreter,
        task.execTrace)
    }

    override def submit(task: Task[F]): F[SubmissionResult] =
      task match {
        case deliverTask @ Deliver(e @ Envelope(sender, event, receiver), _) =>
          ct.suspend {
            context
              .getProcessState(receiver)
              .fold[F[SubmissionResult]](sendUnknownProcessError(deliverTask).map(_ => UnknownProcess)) { ps =>
                event match {
                  case Kill =>
                    logger.debug(s"Scheduler::submit(ps=$ps, task=$task) - interrupted") >>
                      stopProcess(
                        sender = sender,
                        context = context,
                        receiver = receiver,
                        interpreter = interpreter,
                        execTrace = deliverTask.execTrace,
                        logger = logger,
                        onError = (_, err) =>
                          handleError(ps.process, e, context, interpreter, deliverTask.execTrace, err, logger)
                      ).map(_ => Ok)
                  case _ => submit(ps, deliverTask)
                }
              }
          }
        case t => ct.raiseError(new RuntimeException(s"unsupported task: $t"))
      }

    private def createWorkers: List[Worker[F]] =
      (1 to config.numberOfWorkers).map { i =>
        new Worker[F](s"worker-$i", context, signalQueue, interpreter)
      }.toList

  }

  object SchedulerImpl {

    def apply[F[_]: Concurrent: Timer: Parallel: ContextShift](
        config: SchedulerConfig,
        context: Context[F],
        interpreter: Interpreter[F]
    ): F[Scheduler[F]] =
      for {
        signalQueue <- Queue.unbounded[F, Signal](ChannelType.MPMC)
      } yield new SchedulerImpl(config, context, signalQueue, interpreter)

    class Worker[F[_]: Concurrent: Timer: Parallel: ContextShift](
        name: String,
        context: Context[F],
        signalQueue: Queue[F, Signal],
        interpreter: Interpreter[F]
    ) {
      private val logger = LoggerWrapper(Logger(LoggerFactory.getLogger(s"parapet-scheduler-$name")), context.devMode)
      private val ct = implicitly[Concurrent[F]]

      def run: F[Unit] = {
        def step: F[Unit] =
          logger.debug(s"worker[$name] waiting on signalQueue") >>
            signalQueue.dequeue >>= { signal =>
            logger.debug(s"worker[$name] received signal: $signal") >>
              (context.getProcessState(signal.envelop.receiver) match {
                case Some(ps) =>
                  ps.acquire >>= {
                    case true =>
                      logger.debug(s"worker[$name] acquired process: ${signal.envelop.receiver} lock") >>
                        run(ps) >> step
                    case false => step
                  }
                case None =>
                  logger.error(s"worker[$name] no such process. signal: $signal") >>
                    step // process was terminated and removed from the system,
                // eventually scheduler will stop delivering new events for this process
              })

          }

        step
      }

      /** Processing all tasks in the process queue until its empty.
        *
        * @param ps the process
        * @return [[F]] Unit
        */
      private def run(ps: ProcessState[F]): F[Unit] = {
        def step: F[Unit] =
          ps.tryTakeTask >>= {
            case Some(task: Deliver[F]) =>
              if (task.envelope.event.isInstanceOf[Scheduler.Inbox.type]) step
              else deliver(ps, task) >> ps.isBlocking.flatMap(if (_) waitForCompletion(task, ps) else step)
            case Some(task) => ct.raiseError(new UnsupportedOperationException(s"unsupported task type: $task"))
            case None       => releaseWithOptNotify(ps)
          }

        logger.debug(s"worker[$name]::run(${ps.process})") >> step

      }

      private def deliver(ps: ProcessState[F], task: Deliver[F]): F[Unit] = {
        val envelope = task.envelope
        val process = ps.process
        val event = envelope.event
        val sender = envelope.sender

        event match {
          case Stop =>
            ps.stopped.flatMap {
              case true =>
                sendToDeadLetter(
                  DeadLetter(envelope, ProcessStoppedException(process.ref)),
                  context,
                  interpreter,
                  task.execTrace)
              case false =>
                stopProcess(
                  sender,
                  context,
                  process.ref,
                  interpreter,
                  task.execTrace,
                  logger,
                  (_, err) => handleError(process, envelope, context, interpreter, task.execTrace, err, logger)) >>
                  context.remove(process.ref).void
            }
          case _ =>
            ps.terminated.product(ps.stopped).flatMap {
              case (_, true) | (true, _) =>
                sendToDeadLetter(
                  DeadLetter(envelope, new IllegalStateException(s"process=$process is terminated")),
                  context,
                  interpreter,
                  task.execTrace) //>>
              // releaseAndNotify(ps, thisTrace.append("terminated"))
              case _ =>
                ct.delay(Try(process.canHandle(event))).flatMap {
                  case scala.util.Success(true) =>
                    for {
                      flow <- ct.delay(process(event))
                      effect <- ct.pure(flow.foldMap[F](interpreter.interpret(sender, ps, task.execTrace)))
                      _ <- runEffect(
                        effect,
                        envelope,
                        ps,
                        err => handleError(process, envelope, context, interpreter, task.execTrace, err, logger))
                    } yield ()
                  case scala.util.Success(false) =>
                    val errorMsg = s"process $process handler is not defined for event: $event"
                    val whenUndefined = event match {
                      case f: Failure =>
                        // no error handling, send to dead letter
                        sendToDeadLetter(DeadLetter(f), context, interpreter, task.execTrace)
                      case Start => ct.unit // ignore lifecycle events
                      case _ =>
                        send(
                          ProcessRef.SystemRef,
                          Failure(envelope, EventMatchException(errorMsg)),
                          context.getProcessState(envelope.sender).get,
                          interpreter,
                          task.execTrace)
                    }
                    val logMsg = event match {
                      case Start | Stop => ct.unit
                      case _            => logger.warn(errorMsg)
                    }
                    logMsg >> whenUndefined
                  case scala.util.Failure(err) =>
                    logger.error(s"process name=${process.name} " +
                      s"ref=${process.ref} has failed to match event=$event", err)
                }
            }
        }
      }

      /** Waits until all blocking operations completed, releases the process and
        * puts [[NotifyEvent]] into [[signalQueue]].
        * @param deliver th task to process
        * @param ps the process
        * @return [[F]] unit
        */
      private def waitForCompletion(deliver: Deliver[F], ps: ProcessState[F]): F[Unit] =
        for {
          numOfBlockingOps <- ps.blocking.size
          _ <- logger.debug(s"worker[$name]::waits for completion of $numOfBlockingOps blocking operations. " +
            s"process: ${ps.process.ref}")
          _ <- ct.start(
            ct.guarantee {
              runEffect(
                ps.blocking.waitForCompletion,
                deliver.envelope,
                ps,
                err => handleError(ps.process, deliver.envelope, context, interpreter, deliver.execTrace, err, logger))
            } {
              logger.debug(
                s"worker[$name]:: $numOfBlockingOps blocking operations completed. process: ${ps.process.ref}") >>
                ps.blocking.clear >> releaseAndNotify(ps)
            })

        } yield ()

      /** Releases the process and optionally puts [[NotifyEvent]] into [[signalQueue]].
        * see [[Context.ProcessState.ProcessLock]]
        * @param ps the process
        * @return [[F]] unit
        */
      private def releaseWithOptNotify(ps: ProcessState[F]): F[Unit] =
        ps.release.flatMap {
          case true =>
            logger.debug(s"process: ${ps.process.ref} has been released. process has no pending events.")
          case false =>
            logger.debug(
              s"process: ${ps.process.ref} has been released. " +
                "process has some pending events. put notification event into the signal queue."
            ) >> signalQueue.enqueue(createNotifySignal(ps.process.ref))
        }

      /** Releases the process and puts [[NotifyEvent]] into [[signalQueue]].
        * see [[Context.ProcessState.ProcessLock]]
        * @param ps the process
        * @return [[F]] unit
        */
      private def releaseAndNotify(ps: ProcessState[F]): F[Unit] =
        logger.debug(
          s"process: ${ps.process.ref} has been released. put notification event into the signal queue"
        ) >> ps.release.flatMap(_ => signalQueue.enqueue(createNotifySignal(ps.process.ref)))

      private def runEffect(
          effect: F[Unit],
          envelope: Envelope,
          ps: ProcessState[F],
          errorHandler: Throwable => F[Unit]
      ): F[Unit] =
        logger.debug(s"worker[$name]::runEffect. envelop: $envelope") >> runSync(effect, errorHandler)

      private def runSync(
          flow: F[Unit],
          errorHandler: Throwable => F[Unit]
      ): F[Unit] =
        ct.handleErrorWith(flow)(errorHandler)

      private def createNotifySignal(ref: ProcessRef): Signal = {
        val e = Envelope(ProcessRef.SchedulerRef, NotifyEvent, ref)
        Signal(e, context.createTrace(e.id))
      }

    }

    private def handleError[F[_]: Concurrent](
        process: Process[F],
        envelope: Envelope,
        context: Context[F],
        interpreter: Interpreter[F],
        executionTrace: ExecutionTrace,
        cause: Throwable,
        logger: LoggerWrapper[F]
    ): F[Unit] = {

      val event = envelope.event

      event match {
        case f: Failure =>
          logger.error(s"process $process has failed to handle Failure event. send to dead-letter", cause) >>
            sendToDeadLetter(DeadLetter(f), context, interpreter, executionTrace)
        case _ =>
          val errMsg = s"process $process has failed to handle event: $event"
          logger.error(errMsg, cause) >>
            sendErrorToSender(envelope, context, interpreter, executionTrace, EventHandlingException(errMsg, cause))
      }
    }

    private def sendErrorToSender[F[_]: Concurrent](
        envelope: Envelope,
        context: Context[F],
        interpreter: Interpreter[F],
        executionTrace: ExecutionTrace,
        err: Throwable
    ): F[Unit] =
      send(
        SystemRef,
        Failure(envelope, err),
        context.getProcessState(envelope.sender).get,
        interpreter,
        executionTrace
      )

    private def sendToDeadLetter[F[_]: Concurrent](
        dl: DeadLetter,
        context: Context[F],
        interpreter: Interpreter[F],
        executionTrace: ExecutionTrace
    )(implicit
        flowDsl: FlowOps[F, Dsl[F, *]]
    ): F[Unit] =
      send(SystemRef, dl, context.getProcessState(DeadLetterRef).get, interpreter, executionTrace)

    private def send[F[_]: Concurrent](
        sender: ProcessRef,
        event: Event,
        receiver: ProcessState[F],
        interpreter: Interpreter[F],
        execTrace: ExecutionTrace
    )(implicit flowDsl: FlowOps[F, Dsl[F, *]]): F[Unit] =
      flowDsl.send(sender, event, receiver.process.ref).foldMap[F](interpreter.interpret(sender, receiver, execTrace))

    private def deliverStopEvent[F[_]: Concurrent](
        sender: ProcessRef,
        ps: ProcessState[F],
        interpreter: Interpreter[F],
        executionTrace: ExecutionTrace
    ): F[Unit] = {
      val ct = implicitly[Concurrent[F]]
      if (ps.process.canHandle(Stop)) {
        ps.process(Stop).foldMap[F](interpreter.interpret(sender, ps, executionTrace))
      } else {
        ct.unit
      }
    }

    /** Stops the given process and its child processes.
      * @param sender the sender
      * @param context the context
      * @param receiver the process that should be stopped
      * @param interpreter the dsl interpreter
      * @param logger the logger
      * @param execTrace the execution trace
      * @param onError the error handler
      * @tparam F effect
      * @return true if the process was stopped, otherwise - false
      */
    private def stopProcess[F[_]: Concurrent: Parallel](
        sender: ProcessRef,
        context: Context[F],
        receiver: ProcessRef,
        interpreter: Interpreter[F],
        execTrace: ExecutionTrace,
        logger: LoggerWrapper[F],
        onError: (ProcessRef, Throwable) => F[Unit]
    ): F[Boolean] = {
      val ct = implicitly[Concurrent[F]]
      val pa = implicitly[Parallel[F]]

      ct.suspend {
        val stopChildProcesses =
          pa.par(
            context
              .child(receiver)
              .map(child => stopProcess(receiver, context, child, interpreter, execTrace, logger, onError))
          )

        context.getProcessState(receiver) match {
          case Some(ps) =>
            ps.stop().flatMap {
              case true =>
                stopChildProcesses >>
                  ps.blocking.completeAll >>
                  deliverStopEvent(sender, ps, interpreter, execTrace).handleErrorWith(err => onError(receiver, err)) >>
                  logger.debug(s"process: '$receiver' has been stopped") >> ct.pure(true)
              case false =>
                logger.warn(s"process: '$receiver' cannot be stopped b/c it's already stopped") >> ct.pure(false)
            }
          case None =>
            logger.warn(s"process: '$receiver' cannot be stopped b/c it doesn't exist") >> ct.pure(false)
        }
      }
    }

  }

}
