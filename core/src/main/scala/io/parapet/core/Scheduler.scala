package io.parapet.core

import com.typesafe.scalalogging.Logger
import io.parapet.ProcessRef.*
import io.parapet.core.Context.ProcessState
import io.parapet.core.Dsl.{Dsl, FlowOps}
import io.parapet.core.DslInterpreter.Interpreter
import io.parapet.core.Events.*
import io.parapet.core.Queue.ChannelType
import io.parapet.core.Scheduler.*
import io.parapet.core.exceptions.*
import io.parapet.effect.Effect
import io.parapet.effect.Monad.*
import io.parapet.{Envelope, Event, ProcessRef}
import org.slf4j.LoggerFactory

import scala.util.Try

/** Routes [[Scheduler.Task]]s (envelope deliveries) to the right per-process mailbox and
  * runs the resulting handler programs on a pool of worker fibers.
  *
  * The scheduler enforces parapet's core concurrency invariants:
  *   - Per-process serialization: only one worker holds a process's lock at a time.
  *   - Bounded mailboxes: when full, [[submit]] returns [[Scheduler.ProcessQueueIsFull]]
  *     so callers can apply backpressure.
  *   - Cooperative shutdown: stopping a process cascades to descendants via the
  *     [[Context]] supervision graph.
  *
  * Implementations are expected to be thread-safe.
  */
trait Scheduler[F[_]]:
  /** Starts the worker pool. Returns once the pool has begun consuming the signal queue;
    * shutdown is handled by [[Effect.guarantee]] inside the implementation when cancelled.
    */
  def start: F[Unit]

  /** Submits a task for routing to its addressed process. The returned
    * [[Scheduler.SubmissionResult]] indicates whether delivery is guaranteed
    * ([[Scheduler.Ok]]) or has been refused (queue full, unknown process, etc.).
    */
  def submit(task: Task[F]): F[SubmissionResult]

/** Companion holding scheduler primitives, configuration, and the default implementation. */
object Scheduler:
  /** Internal heartbeat event used to wake worker fibers without delivering a payload. */
  case object Inbox extends Event

  /** A single notification placed on the scheduler's signal queue, instructing workers to
    * pull from the addressed process's mailbox.
    */
  final case class Signal(envelope: Envelope, execTrace: ExecutionTrace):
    override def toString: String =
      s"Signal($envelope, $execTrace)"

  /** Algebra of work items the scheduler accepts. Currently only [[Deliver]] but the type
    * is sealed to allow future expansion (e.g. timers, cron).
    */
  sealed trait Task[F[_]]

  /** Deliver `envelope` to its addressed process; `execTrace` carries causal id. */
  final case class Deliver[F[_]](envelope: Envelope, execTrace: ExecutionTrace) extends Task[F]

  /** A mailbox holding [[Task]]s for a single process. */
  type TaskQueue[F[_]] = Queue[F, Task[F]]

  /** Builds the default [[SchedulerImpl]] with an unbounded MPMC signal queue. */
  def apply[F[_]](
      config: SchedulerConfig,
      context: Context[F],
      interpreter: Interpreter[F]
  )(using effect: Effect[F], parallel: Parallel[F]): F[Scheduler[F]] =
    SchedulerImpl.apply(config, context, interpreter)

  /** @param numberOfWorkers number of worker fibers to spin up; must be positive. */
  final case class SchedulerConfig(numberOfWorkers: Int):
    require(numberOfWorkers > 0)

  object SchedulerConfig:
    /** Defaults to one worker per available CPU. */
    val default: SchedulerConfig =
      SchedulerConfig(numberOfWorkers = Runtime.getRuntime.availableProcessors())

  /** Thin wrapper around an SLF4J [[Logger]] that elides invocations when not in
    * [[Parapet.ParConfig.devMode]] — keeps hot paths from constructing message strings
    * unnecessarily.
    */
  final case class LoggerWrapper[F[_]](logger: Logger, devMode: Boolean)(using effect: Effect[F]):
    def debug(message: => String): F[Unit] =
      if devMode then effect.delay(logger.debug(message)) else effect.pure(())

    def error(message: => String): F[Unit] =
      if devMode then effect.delay(logger.error(message)) else effect.pure(())

    def error(message: => String, cause: Throwable): F[Unit] =
      if devMode then effect.delay(logger.error(message, cause)) else effect.pure(())

    def info(message: => String): F[Unit] =
      if devMode then effect.delay(logger.info(message)) else effect.pure(())

    def warn(message: => String): F[Unit] =
      if devMode then effect.delay(logger.warn(message)) else effect.pure(())

  /** Internal "wake up and check the mailbox" event the scheduler enqueues after a release
    * if there are still pending tasks.
    */
  private[core] case object NotifyEvent extends Event

  /** Outcome of [[Scheduler.submit]]. */
  sealed trait SubmissionResult

  /** Accepted; delivery will happen. */
  case object Ok extends SubmissionResult

  /** Rejected: the receiver's [[ProcessRef]] is not registered. */
  case object UnknownProcess extends SubmissionResult

  /** Rejected: the receiver has been terminated. */
  case object TerminatedProcess extends SubmissionResult

  /** Rejected: the receiver's mailbox is full. The sender should back off and retry. */
  case object ProcessQueueIsFull extends SubmissionResult

  /** Default [[Scheduler]] implementation backed by a pool of [[SchedulerImpl.Worker]]
    * fibers reading from a single shared signal queue.
    *
    * Tasks land in per-process mailboxes via [[submit]]. Workers race for the per-process
    * lock and drain the mailbox sequentially, ensuring per-process serialization while
    * still parallelizing across processes.
    */
  final class SchedulerImpl[F[_]](
      config: SchedulerConfig,
      context: Context[F],
      signalQueue: Queue[F, Signal],
      interpreter: Interpreter[F]
  )(using effect: Effect[F], parallel: Parallel[F])
      extends Scheduler[F]:

    private val logger = LoggerWrapper(Logger(LoggerFactory.getLogger(getClass.getCanonicalName)), context.devMode)

    def start: F[Unit] =
      effect.guarantee(effect.delay(createWorkers).flatMap(workers => parallel.par(workers.map(_.run)))) {
        SchedulerImpl.stopProcess(
          sender = ProcessRef.SystemRef,
          context = context,
          receiver = ProcessRef.SystemRef,
          interpreter = interpreter,
          execTrace = context.createTrace,
          logger = logger,
          onError = (processRef, error) => logger.error(s"An error occurred while stopping process $processRef", error)
        ) >> context.saveEventLog >> logger.info("scheduler has been shut down")
      }

    private def submit(processState: ProcessState[F], task: Deliver[F]): F[SubmissionResult] =
      processState.tryPut(task).flatMap {
        case true =>
          logger.debug(s"Scheduler::submit(ps=${processState.process}, task=$task) - task added to the process queue") >>
            processState.acquired.flatMap {
              case true =>
                logger.debug(
                  s"Scheduler::submit(ps=${processState.process}, task=$task) - lock is already acquired. do not notify"
                )
              case false =>
                signalQueue.enqueue(Signal(task.envelope, task.execTrace)) >>
                  logger.debug(
                    s"Scheduler::submit(ps=${processState.process}, task=$task) - added to notification queue. traceId:${task.execTrace.last}"
                  )
            }.as(Ok)
        case false =>
          logger.warn(s"process ${processState.process} event queue is full").as(ProcessQueueIsFull)
      }

    def sendUnknownProcessError(task: Deliver[F]): F[Unit] =
      val envelope = task.envelope
      SchedulerImpl.send(
        SystemRef,
        Failure(envelope, UnknownProcessException(s"there is no such process with id=${envelope.receiver} registered in the system")),
        context.getProcessState(envelope.sender).get,
        interpreter,
        task.execTrace
      )

    def submit(task: Task[F]): F[SubmissionResult] =
      task match
        case deliverTask @ Deliver(envelope @ Envelope(sender, event, receiver), _) =>
          effect.suspend(
            context.getProcessState(receiver) match
              case None =>
                sendUnknownProcessError(deliverTask).as(UnknownProcess)
              case Some(processState) =>
                event match
                  case Kill =>
                    logger.debug(s"Scheduler::submit(ps=$processState, task=$task) - interrupted") >>
                      SchedulerImpl.stopProcess(
                        sender = sender,
                        context = context,
                        receiver = receiver,
                        interpreter = interpreter,
                        execTrace = deliverTask.execTrace,
                        logger = logger,
                        onError = (_, error) =>
                          SchedulerImpl.handleError(
                            processState.process,
                            envelope,
                            context,
                            interpreter,
                            deliverTask.execTrace,
                            error,
                            logger
                          )
                      ).as(Ok)
                  case _ =>
                    submit(processState, deliverTask)
          )
        case unsupported =>
          effect.raiseError(new RuntimeException(s"unsupported task: $unsupported"))

    private def createWorkers: List[SchedulerImpl.Worker[F]] =
      (1 to config.numberOfWorkers)
        .map(index => SchedulerImpl.Worker[F](s"worker-$index", context, signalQueue, interpreter))
        .toList

  /** Helpers and the [[Worker]] type for [[SchedulerImpl]]. */
  object SchedulerImpl:
    /** Builds an MPMC-backed [[SchedulerImpl]]. */
    def apply[F[_]](
        config: SchedulerConfig,
        context: Context[F],
        interpreter: Interpreter[F]
    )(using effect: Effect[F], parallel: Parallel[F]): F[Scheduler[F]] =
      Queue.unbounded[F, Signal](ChannelType.MPMC)
        .map(signalQueue => new SchedulerImpl[F](config, context, signalQueue, interpreter))

    /** A worker fiber that pulls from `signalQueue`, races for per-process locks, and
      * drains the winner's mailbox.
      *
      * Multiple workers run in parallel; per-process exclusivity is provided by the
      * cooperative lock in [[Context.ProcessState]].
      */
    final case class Worker[F[_]](
        name: String,
        context: Context[F],
        signalQueue: Queue[F, Signal],
        interpreter: Interpreter[F]
    )(using effect: Effect[F], parallel: Parallel[F]):
      private val logger = LoggerWrapper(Logger(LoggerFactory.getLogger(s"parapet-scheduler-$name")), context.devMode)

      def run: F[Unit] =
        def step: F[Unit] =
          logger.debug(s"worker[$name] waiting on signalQueue") >>
            signalQueue.dequeue.flatMap { signal =>
              logger.debug(s"worker[$name] received signal: $signal") >>
                (context.getProcessState(signal.envelope.receiver) match
                  case Some(processState) =>
                    processState.acquire.flatMap {
                      case true =>
                        logger.debug(s"worker[$name] acquired process: ${signal.envelope.receiver} lock") >>
                          run(processState) >> step
                      case false =>
                        step
                    }
                  case None =>
                    logger.error(s"worker[$name] no such process. signal: $signal") >> step)
            }

        step

      private def run(processState: ProcessState[F]): F[Unit] =
        def step: F[Unit] =
          processState.tryTakeTask.flatMap {
            case Some(task: Deliver[F]) =>
              if task.envelope.event.isInstanceOf[Scheduler.Inbox.type] then step
              else deliver(processState, task) >> processState.isBlocking.flatMap {
                case true  => waitForCompletion(task, processState)
                case false => step
              }
            case Some(task) =>
              effect.raiseError(new UnsupportedOperationException(s"unsupported task type: $task"))
            case None =>
              releaseWithOptNotify(processState)
          }

        logger.debug(s"worker[$name]::run(${processState.process})") >> step

      private def deliver(processState: ProcessState[F], task: Deliver[F]): F[Unit] =
        val envelope = task.envelope
        val process = processState.process
        val event = envelope.event
        val sender = envelope.sender

        event match
          case Stop =>
            processState.stopped.flatMap {
              case true =>
                sendToDeadLetter(DeadLetter(envelope, ProcessStoppedException(process.ref)), context, interpreter, task.execTrace)
              case false =>
                stopProcess(
                  sender,
                  context,
                  process.ref,
                  interpreter,
                  task.execTrace,
                  logger,
                  (_, error) => handleError(process, envelope, context, interpreter, task.execTrace, error, logger)
                ) >> context.remove(process.ref).void
            }
          case _ =>
            processState.terminated.flatMap { terminated =>
              processState.stopped.flatMap { stopped =>
                if terminated || stopped then
                  sendToDeadLetter(
                    DeadLetter(envelope, new IllegalStateException(s"process=$process is terminated")),
                    context,
                    interpreter,
                    task.execTrace
                  )
                else
                  effect.delay(Try(process.canHandle(event))).flatMap {
                    case scala.util.Success(true) =>
                      for
                        flow <- effect.delay(process(event))
                        effect0 <- effect.pure(flow.foldMap(interpreter.interpret(sender, processState, task.execTrace)))
                        _ <- runEffect(
                          effect0,
                          envelope,
                          processState,
                          error => handleError(process, envelope, context, interpreter, task.execTrace, error, logger)
                        )
                      yield ()

                    case scala.util.Success(false) =>
                      val errorMessage = s"process $process handler is not defined for event: $event"
                      val whenUndefined: F[Unit] = event match
                        case failure: Failure =>
                          sendToDeadLetter(DeadLetter(failure), context, interpreter, task.execTrace)
                        case Start =>
                          effect.pure(())
                        case _ =>
                          send(
                            ProcessRef.SystemRef,
                            Failure(envelope, EventMatchException(errorMessage)),
                            context.getProcessState(envelope.sender).get,
                            interpreter,
                            task.execTrace
                          )

                      val logMessage = event match
                        case Start | Stop => effect.pure(())
                        case _            => logger.warn(errorMessage)

                      logMessage >> whenUndefined

                    case scala.util.Failure(error) =>
                      logger.error(
                        s"process name=${process.name} ref=${process.ref} has failed to match event=$event",
                        error
                      )
                  }
              }
            }

      private def waitForCompletion(deliver: Deliver[F], processState: ProcessState[F]): F[Unit] =
        for
          numberOfBlockingOps <- processState.blocking.size
          _ <- logger.debug(
            s"worker[$name]::waits for completion of $numberOfBlockingOps blocking operations. process: ${processState.process.ref}"
          )
          _ <- effect.start(
            effect.guarantee(
              runEffect(
                processState.blocking.waitForCompletion,
                deliver.envelope,
                processState,
                error => handleError(processState.process, deliver.envelope, context, interpreter, deliver.execTrace, error, logger)
              )
            ) {
              logger.debug(
                s"worker[$name]:: $numberOfBlockingOps blocking operations completed. process: ${processState.process.ref}"
              ) >> processState.blocking.clear >> releaseAndNotify(processState)
            }
          )
        yield ()

      private def releaseWithOptNotify(processState: ProcessState[F]): F[Unit] =
        processState.release.flatMap {
          case true =>
            logger.debug(s"process: ${processState.process.ref} has been released. process has no pending events.")
          case false =>
            logger.debug(
              s"process: ${processState.process.ref} has been released. process has some pending events. put notification event into the signal queue."
            ) >> signalQueue.enqueue(createNotifySignal(processState.process.ref))
        }

      private def releaseAndNotify(processState: ProcessState[F]): F[Unit] =
        logger.debug(
          s"process: ${processState.process.ref} has been released. put notification event into the signal queue"
        ) >> processState.release.flatMap(_ => signalQueue.enqueue(createNotifySignal(processState.process.ref)))

      private def runEffect(
          effect0: F[Unit],
          envelope: Envelope,
          processState: ProcessState[F],
          errorHandler: Throwable => F[Unit]
      ): F[Unit] =
        logger.debug(s"worker[$name]::runEffect. envelope: $envelope") >> effect0.handleErrorWith(errorHandler)

      private def createNotifySignal(ref: ProcessRef): Signal =
        val envelope = Envelope(ProcessRef.SchedulerRef, NotifyEvent, ref)
        Signal(envelope, context.createTrace(envelope.id))

    private def handleError[F[_]](
        process: Process[F],
        envelope: Envelope,
        context: Context[F],
        interpreter: Interpreter[F],
        executionTrace: ExecutionTrace,
        cause: Throwable,
        logger: LoggerWrapper[F]
    )(using effect: Effect[F]): F[Unit] =
      envelope.event match
        case failure: Failure =>
          logger.error(s"process $process has failed to handle Failure event. send to dead-letter", cause) >>
            sendToDeadLetter(DeadLetter(failure), context, interpreter, executionTrace)
        case event =>
          val errorMessage = s"process $process has failed to handle event: $event"
          logger.error(errorMessage, cause) >>
            sendErrorToSender(envelope, context, interpreter, executionTrace, EventHandlingException(errorMessage, cause))

    private def sendErrorToSender[F[_]](
        envelope: Envelope,
        context: Context[F],
        interpreter: Interpreter[F],
        executionTrace: ExecutionTrace,
        error: Throwable
    )(using effect: Effect[F]): F[Unit] =
      send(SystemRef, Failure(envelope, error), context.getProcessState(envelope.sender).get, interpreter, executionTrace)

    private def sendToDeadLetter[F[_]](
        deadLetter: DeadLetter,
        context: Context[F],
        interpreter: Interpreter[F],
        executionTrace: ExecutionTrace
    )(using effect: Effect[F], flowOps: FlowOps[F, [x] =>> Dsl[F, x]]): F[Unit] =
      send(SystemRef, deadLetter, context.getProcessState(DeadLetterRef).get, interpreter, executionTrace)

    private def send[F[_]](
        sender: ProcessRef,
        event: Event,
        receiver: ProcessState[F],
        interpreter: Interpreter[F],
        execTrace: ExecutionTrace
    )(using effect: Effect[F], flowOps: FlowOps[F, [x] =>> Dsl[F, x]]): F[Unit] =
      flowOps.send(sender, event, receiver.process.ref).foldMap(interpreter.interpret(sender, receiver, execTrace))

    private def deliverStopEvent[F[_]](
        sender: ProcessRef,
        processState: ProcessState[F],
        interpreter: Interpreter[F],
        executionTrace: ExecutionTrace
    )(using effect: Effect[F]): F[Unit] =
      if processState.process.canHandle(Stop) then
        processState.process(Stop).foldMap(interpreter.interpret(sender, processState, executionTrace))
      else effect.pure(())

    private def stopProcess[F[_]](
        sender: ProcessRef,
        context: Context[F],
        receiver: ProcessRef,
        interpreter: Interpreter[F],
        execTrace: ExecutionTrace,
        logger: LoggerWrapper[F],
        onError: (ProcessRef, Throwable) => F[Unit]
    )(using effect: Effect[F], parallel: Parallel[F]): F[Boolean] =
      effect.suspend {
        val stopChildProcesses =
          parallel.par(context.child(receiver).map(child => stopProcess(receiver, context, child, interpreter, execTrace, logger, onError).void))

        context.getProcessState(receiver) match
          case Some(processState) =>
            processState.stop().flatMap {
              case true =>
                stopChildProcesses >>
                  processState.blocking.completeAll >>
                  deliverStopEvent(sender, processState, interpreter, execTrace)
                    .handleErrorWith(error => onError(receiver, error)) >>
                  logger.debug(s"process: '$receiver' has been stopped") >>
                  effect.pure(true)
              case false =>
                logger.warn(s"process: '$receiver' cannot be stopped because it is already stopped") >>
                  effect.pure(false)
            }
          case None =>
            logger.warn(s"process: '$receiver' cannot be stopped because it does not exist") >>
              effect.pure(false)
      }
