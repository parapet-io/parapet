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

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import scala.util.Try

/** Routes [[Scheduler.Task]]s (envelope deliveries) to the right per-process mailbox and runs the resulting handler
  * programs on a pool of worker fibers.
  *
  * The scheduler enforces parapet's core concurrency invariants:
  *   - Per-process serialization: only one worker holds a process's lock at a time.
  *   - Bounded mailboxes: when full, [[submit]] returns [[Scheduler.ProcessQueueIsFull]] so callers can apply
  *     backpressure.
  *   - Cooperative shutdown: stopping a process cascades to descendants via the [[Context]] supervision graph.
  *
  * Implementations are expected to be thread-safe.
  */
trait Scheduler[F[_]]:
  /** Starts the worker pool. Returns once the pool has begun consuming the signal queue; shutdown is handled by
    * [[Effect.guarantee]] inside the implementation when cancelled.
    */
  def start: F[Unit]

  /** Submits a task for routing to its addressed process. The returned [[Scheduler.SubmissionResult]] indicates whether
    * delivery is guaranteed ([[Scheduler.Ok]]) or has been refused (queue full, unknown process, etc.).
    */
  def submit(task: Task[F]): F[SubmissionResult]

/** Companion holding scheduler primitives, configuration, and the default implementation. */
object Scheduler:
  /** Internal heartbeat event used to wake worker fibers without delivering a payload. */
  case object Inbox extends Event

  /** A single notification placed on the scheduler's signal queue, instructing workers to pull from the addressed
    * process's mailbox.
    */
  final case class Signal(envelope: Envelope):
    override def toString: String =
      s"Signal($envelope)"

  /** Algebra of work items the scheduler accepts. Currently only [[Deliver]] but the type is sealed to allow future
    * expansion (e.g. timers, cron).
    */
  sealed trait Task[F[_]]

  /** Deliver `envelope` to its addressed process. */
  final case class Deliver[F[_]](envelope: Envelope) extends Task[F]

  /** A mailbox holding [[Task]]s for a single process. */
  type TaskQueue[F[_]] = Queue[F, Task[F]]

  /** Builds the default [[SchedulerImpl]] with an unbounded MPMC signal queue. */
  def apply[F[_]](
      config: SchedulerConfig,
      context: Context[F],
      interpreter: Interpreter[F]
  )(using effect: Effect[F], parallel: Parallel[F], schedulerRuntime: SchedulerRuntime[F]): F[Scheduler[F]] =
    SchedulerImpl.apply(config, context, interpreter)

  /** @param numberOfWorkers
    *   number of worker fibers to spin up; must be positive.
    * @param numberOfSignalQueues
    *   number of signal queues the scheduler uses. With `1` every worker blocks on a single MPMC queue and every
    *   producer enqueues onto it. With `N > 1` the queue is sharded into `N` independent MPMC queues: producers
    *   round-robin their submissions across them and workers prefer their own home queue with work stealing as a
    *   fallback. The effective queue count is `min(numberOfSignalQueues, numberOfWorkers)`. The recommended ratio is
    *   `numberOfSignalQueues == numberOfWorkers` - it gives every worker its own home queue (no consumer-side
    *   contention) and spreads producer pressure across `N` shards. Values smaller than `numberOfWorkers` are also
    *   legal; multiple workers will share a home queue and contend on its head.
    * @param mailboxSlice
    *   maximum number of events a single worker may drain from one process's mailbox before yielding back to the signal
    *   queue. Bounds how long a process with a long backlog can monopolize its worker, which keeps the scheduler fair
    *   when a slow / chatty process shares a worker pool with quick ones. Set to `Int.MaxValue` to drain entire
    *   mailboxes without yielding.
    */
  final case class SchedulerConfig(
      numberOfWorkers: Int,
      numberOfSignalQueues: Int = 1,
      mailboxSlice: Int = 256
  ):
    require(numberOfWorkers > 0, s"numberOfWorkers must be positive, got $numberOfWorkers")
    require(numberOfSignalQueues > 0, s"numberOfSignalQueues must be positive, got $numberOfSignalQueues")
    require(mailboxSlice > 0, s"mailboxSlice must be positive, got $mailboxSlice")

  object SchedulerConfig:
    /** Defaults to one worker per available CPU and one signal queue per worker.
      *
      * The 1:1 worker-to-queue ratio gives every worker its own home queue (zero consumer-side contention) and lets
      * producers round-robin across `N` shards so per-queue producer pressure is `producers / N`. The work-stealing
      * fallback in [[SchedulerImpl.Worker.nextSignal]] keeps the topology correct under skew.
      */
    val default: SchedulerConfig =
      val cores = Runtime.getRuntime.availableProcessors()
      SchedulerConfig(numberOfWorkers = cores, numberOfSignalQueues = cores)

  /** Thin wrapper around an SLF4J [[Logger]] that elides invocations when not in [[Parapet.ParConfig.devMode]] - keeps
    * hot paths from constructing message strings unnecessarily.
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

  /** Internal "wake up and check the mailbox" event the scheduler enqueues after a release if there are still pending
    * tasks.
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

  /** Default [[Scheduler]] implementation backed by a pool of [[SchedulerImpl.Worker]] fibers reading from one or more
    * signal queues.
    *
    * Tasks land in per-process mailboxes via [[submit]]. The mailbox is the authoritative source of events; the signal
    * queue is purely a wake-up mechanism. A worker that wins the race for a process's lock drains that mailbox
    * sequentially, guaranteeing per-process serialization while still parallelizing across processes.
    *
    * ### Signal-queue topology
    *
    * When `SchedulerConfig.numberOfSignalQueues == 1` every worker blocks on a single shared queue and every producer
    * enqueues onto it.
    *
    * When `numberOfSignalQueues > 1` the queue is sharded:
    *   - Producers select a queue round-robin via [[submitCounter]] (one atomic increment per submit).
    *   - Each worker is assigned a **home queue** via `workerIndex % signalQueueCount`. It prefers its own queue and
    *     falls back to a single-pass **work-stealing scan** across siblings before blocking on home.
    *   - Correctness is preserved regardless of topology because the per-process lock still serializes execution. A
    *     stolen signal that loses the acquire race simply falls through and the mailbox remains consistent.
    */
  final class SchedulerImpl[F[_]](
      config: SchedulerConfig,
      context: Context[F],
      signalQueues: Vector[Queue[F, Signal]],
      interpreter: Interpreter[F]
  )(using effect: Effect[F], parallel: Parallel[F], schedulerRuntime: SchedulerRuntime[F])
      extends Scheduler[F]:

    private val logger = LoggerWrapper(Logger(LoggerFactory.getLogger(getClass.getCanonicalName)), context.devMode)

    private val numberOfQueues: Int = signalQueues.length

    /** Round-robin cursor for [[selectSubmitQueue]]. Long-typed so overflow is a non-concern.
      */
    private val submitCounter: java.util.concurrent.atomic.AtomicLong =
      new java.util.concurrent.atomic.AtomicLong(0L)

    /** Chooses a signal queue for a newly submitted signal via round-robin.
      *
      * `Math.floorMod` is used (rather than `%`) so the index stays in `[0, numberOfQueues)` even after the counter
      * eventually goes negative on overflow - it never will in any realistic deployment, but the code is correct
      * without relying on that.
      *
      * Single-queue installations bypass the counter. Multi-queue installations pay exactly one atomic increment per
      * submit regardless of the producer count.
      */
    private def selectSubmitQueue(): Queue[F, Signal] =
      if numberOfQueues == 1 then signalQueues(0)
      else signalQueues(Math.floorMod(submitCounter.getAndIncrement(), numberOfQueues))

    override def start: F[Unit] =
      val runWorkers =
        effect.delay(createWorkers).flatMap(workers => schedulerRuntime.runSchedulerWorkers(workers.map(_.run)))

      effect.guarantee(runWorkers) {
        SchedulerImpl.stopProcess(
          sender = ProcessRef.SystemRef,
          context = context,
          receiver = ProcessRef.SystemRef,
          interpreter = interpreter,
          cause = 0L,
          logger = logger,
          onError = (processRef, error) => logger.error(s"An error occurred while stopping process $processRef", error)
        ) >> context.saveEventLog >> logger.info("scheduler has been shut down")
      }

    private def submit(processState: ProcessState[F], task: Deliver[F]): F[SubmissionResult] =
      processState.tryPut(task).flatMap {
        case true =>
          logger.debug(
            s"Scheduler::submit(ps=${processState.process}, task=$task) - task added to the process queue"
          ) >>
            processState.acquired
              .flatMap {
                case true =>
                  logger.debug(
                    s"Scheduler::submit(ps=${processState.process}, task=$task) - lock is already acquired. do not notify"
                  )
                case false =>
                  effect.suspend(selectSubmitQueue().enqueue(Signal(task.envelope))) >>
                    logger.debug(
                      s"Scheduler::submit(ps=${processState.process}, task=$task) - added to notification queue. id:${task.envelope.id}"
                    )
              }
              .as(Ok)
        case false =>
          logger.warn(s"process ${processState.process} event queue is full").as(ProcessQueueIsFull)
      }

    def sendUnknownProcessError(task: Deliver[F]): F[Unit] =
      val envelope = task.envelope
      SchedulerImpl.send(
        SystemRef,
        Failure(
          envelope,
          UnknownProcessException(s"there is no such process with id=${envelope.receiver} registered in the system")
        ),
        context.getProcessState(envelope.sender).get,
        interpreter,
        envelope.id
      )

    def submit(task: Task[F]): F[SubmissionResult] =
      task match
        case deliverTask @ Deliver(envelope @ Envelope.Routing(sender, event, receiver)) =>
          effect.suspend(
            context.getProcessState(receiver) match
              case None =>
                sendUnknownProcessError(deliverTask).as(UnknownProcess)
              case Some(processState) =>
                event match
                  case Kill =>
                    logger.debug(s"Scheduler::submit(ps=$processState, task=$task) - interrupted") >>
                      SchedulerImpl
                        .stopProcess(
                          sender = sender,
                          context = context,
                          receiver = receiver,
                          interpreter = interpreter,
                          cause = envelope.id,
                          logger = logger,
                          onError = (_, error) =>
                            SchedulerImpl.handleError(
                              processState.process,
                              envelope,
                              context,
                              interpreter,
                              envelope.id,
                              error,
                              logger
                            )
                        )
                        .as(Ok)
                  case _ =>
                    submit(processState, deliverTask)
          )
        case unsupported =>
          effect.raiseError(new RuntimeException(s"unsupported task: $unsupported"))

    private def createWorkers: List[SchedulerImpl.Worker[F]] =
      (0 until config.numberOfWorkers)
        .map(index =>
          SchedulerImpl.Worker[F](
            name = s"worker-${index + 1}",
            homeQueueIdx = index % numberOfQueues,
            mailboxSlice = config.mailboxSlice,
            context = context,
            signalQueues = signalQueues,
            interpreter = interpreter
          )
        )
        .toList

  /** Helpers and the [[Worker]] type for [[SchedulerImpl]]. */
  object SchedulerImpl:

    /** How long an idle worker waits on its home queue before waking up to re-check for work.
      */
    private val IdleWakeupInterval: FiniteDuration = 50.millis

    def apply[F[_]](
        config: SchedulerConfig,
        context: Context[F],
        interpreter: Interpreter[F]
    )(using effect: Effect[F], parallel: Parallel[F], schedulerRuntime: SchedulerRuntime[F]): F[Scheduler[F]] =
      // Cap the queue count at the worker count so every queue has a home worker by index. Configurations with
      // `numberOfSignalQueues > numberOfWorkers` are silently truncated; producers will still round-robin across
      // the surviving queues.
      val signalQueueCount = math.min(config.numberOfSignalQueues, config.numberOfWorkers)

      io.parapet.effect.Monad
        .sequence(List.fill(signalQueueCount)(Queue.unbounded[F, Signal](ChannelType.MPMC)))
        .map(queues => new SchedulerImpl[F](config, context, queues.toVector, interpreter))

    /** A worker fiber that pulls signals from its home queue (or steals from siblings), races for per-process locks,
      * and drains the acquired process mailbox.
      *
      * Multiple workers run in parallel; per-process exclusivity is provided by the cooperative lock in
      * [[Context.ProcessState]], independent of which signal queue surfaced the event.
      *
      * @param homeQueueIdx
      *   index of this worker's preferred signal queue within `signalQueues`.
      * @param signalQueues
      *   all signal queues registered with the scheduler. When size is 1 the worker collapses to a blocking `dequeue`
      *   on the single queue (no work-stealing scan).
      */
    final case class Worker[F[_]](
        name: String,
        homeQueueIdx: Int,
        mailboxSlice: Int,
        context: Context[F],
        signalQueues: Vector[Queue[F, Signal]],
        interpreter: Interpreter[F]
    )(using effect: Effect[F], parallel: Parallel[F]):
      private val logger = LoggerWrapper(Logger(LoggerFactory.getLogger(s"parapet-scheduler-$name")), context.devMode)
      private val numOfQueues = signalQueues.length
      private val homeQueue   = signalQueues(homeQueueIdx)

      /** Attempts to steal a signal from any sibling queue. Does a single pass over the siblings starting immediately
        * after the home index, returning the first non-empty `tryDequeue` result.
        */
      private def trySteal: F[Option[Signal]] = {
        def attempt(cursor: Int, remaining: Int): F[Option[Signal]] =
          if remaining <= 0 then effect.pure(None)
          else
            signalQueues(cursor).tryDequeue.flatMap {
              case some @ Some(_) => effect.pure(some)
              case None           => attempt((cursor + 1) % numOfQueues, remaining - 1)
            }

        attempt((homeQueueIdx + 1) % numOfQueues, numOfQueues - 1)
      }

      /** Fetches the next signal to process: home -> steal -> bounded wait on home, then retry. */
      private def nextSignal: F[Signal] =
        if numOfQueues == 1 then homeQueue.dequeue
        else
          def loop: F[Signal] =
            homeQueue.tryDequeue.flatMap {
              case Some(signal) => effect.pure(signal)
              case None         =>
                trySteal.flatMap {
                  case Some(signal) => effect.pure(signal)
                  case None         =>
                    homeQueue.tryDequeue(SchedulerImpl.IdleWakeupInterval).flatMap {
                      case Some(signal) => effect.pure(signal)
                      case None         => loop
                    }
                }
            }
          loop

      def run: F[Unit] =
        def step: F[Unit] =
          logger.debug(s"worker[$name] waiting for a signal (home=$homeQueueIdx, queues=$numOfQueues)") >>
            nextSignal.flatMap { signal =>
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
                    // Benign teardown race: a process can be removed (e.g. after Stop) while leftover signals for it
                    // are still in the signal queue. The mailbox is the source of truth, signals are only wake-up
                    // tokens, so a stale signal here means "wake-up for an already-drained-and-removed process" - no
                    // event is lost. Logged at warn (not error) to avoid noise during normal lifecycle.
                    logger.warn(s"worker[$name] no such process. signal: $signal") >> step)
            }

        step

      /** Drains up to [[mailboxSlice]] events from `processState`'s mailbox before yielding back to the signal queue.
        * The slice provides cooperative fairness: a process with a long backlog cannot monopolize its worker while
        * sibling processes wait for service.
        *
        * `Inbox` heartbeats are scheduler-internal wake-ups, not user events, so they do not consume slice budget. A
        * `blocking { ... }` handoff exits the slice early via [[waitForCompletion]] regardless of how many events have
        * been processed; the completion fiber re-arms the notify pathway.
        *
        * Two distinct release/notify protocols are used:
        *
        *   - **Empty mailbox** ([[releaseWithOptNotify]]). The submit-side optimization in [[submit]] skips notify
        *     whenever it observes the lock held, on the assumption that the running drainer will drain to empty before
        *     releasing. The lock's sentinel mediates that handshake. Therefore, when the mailbox is genuinely empty,
        *     the standard release-then-maybe-notify path is correct: it notifies iff a submitter raced with us
        *     (sentinel set), and otherwise releases cleanly.
        *   - **Slice exhausted with mailbox potentially non-empty** ([[releaseAndNotify]]). We are yielding mid-drain.
        *     The submit-side optimization may have already skipped notifies (because we held the lock), and the
        *     sentinel may have been cleared by an earlier slice boundary, so we cannot rely on `release`'s return value
        *     to decide whether a wake-up is owed. We must unconditionally re-notify so that some worker resumes the
        *     remaining mailbox. The cost is at most one wasted signal per slice when the mailbox happens to be empty at
        *     the boundary - bounded by `1 / mailboxSlice`.
        *
        * Per-(submitter, receiver) FIFO is preserved across slice boundaries because the mailbox is the single source
        * of order; whichever worker wins the next acquire continues draining from where the previous left off.
        */
      private def run(processState: ProcessState[F]): F[Unit] =
        def step(remaining: Int): F[Unit] =
          if remaining <= 0 then
            logger.debug(
              s"worker[$name]::run(${processState.process}) slice exhausted; yielding back to signal queue"
            ) >> releaseAndNotify(processState)
          else
            processState.tryTakeTask.flatMap {
              case Some(task: Deliver[F]) =>
                if task.envelope.event.isInstanceOf[Scheduler.Inbox.type] then step(remaining)
                else
                  deliver(processState, task) >> processState.hasOffloads.flatMap {
                    case true  => waitForCompletion(task, processState)
                    case false => step(remaining - 1)
                  }
              case Some(task) =>
                effect.raiseError(new UnsupportedOperationException(s"unsupported task type: $task"))
              case None =>
                releaseWithOptNotify(processState)
            }

        logger.debug(s"worker[$name]::run(${processState.process}) slice=$mailboxSlice") >> step(mailboxSlice)

      private def deliver(processState: ProcessState[F], task: Deliver[F]): F[Unit] =
        val envelope = task.envelope
        val process  = processState.process
        val event    = envelope.event
        val sender   = envelope.sender

        event match
          case Stop =>
            processState.stopped.flatMap {
              case true =>
                sendToDeadLetter(
                  DeadLetter(envelope, ProcessStoppedException(process.ref)),
                  context,
                  interpreter,
                  envelope.id
                )
              case false =>
                stopProcess(
                  sender,
                  context,
                  process.ref,
                  interpreter,
                  envelope.id,
                  logger,
                  (_, error) => handleError(process, envelope, context, interpreter, envelope.id, error, logger)
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
                    envelope.id
                  )
                else
                  effect.delay(Try(process.canHandle(event))).flatMap {
                    case scala.util.Success(true) =>
                      for
                        flow    <- effect.delay(process(event))
                        effect0 <- effect.pure(
                          flow.foldMap(interpreter.interpret(sender, processState, envelope.id, envelope.scope))
                        )
                        _ <- runEffect(
                          effect0,
                          envelope,
                          processState,
                          error => handleError(process, envelope, context, interpreter, envelope.id, error, logger)
                        )
                      yield ()

                    case scala.util.Success(false) =>
                      val errorMessage           = s"process $process handler is not defined for event: $event"
                      val whenUndefined: F[Unit] = event match
                        case failure: Failure =>
                          sendToDeadLetter(DeadLetter(failure), context, interpreter, envelope.id)
                        case Start =>
                          effect.pure(())
                        case _ =>
                          send(
                            ProcessRef.SystemRef,
                            Failure(envelope, EventMatchException(errorMessage)),
                            context.getProcessState(envelope.sender).get,
                            interpreter,
                            envelope.id
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
          offloadCount <- processState.offloads.size
          _            <- logger.debug(
            s"worker[$name]::waits for completion of $offloadCount offloaded operations. process: ${processState.process.ref}"
          )
          _ <- effect.start(
            effect.guarantee(
              runEffect(
                processState.offloads.waitForCompletion,
                deliver.envelope,
                processState,
                error =>
                  handleError(
                    processState.process,
                    deliver.envelope,
                    context,
                    interpreter,
                    deliver.envelope.id,
                    error,
                    logger
                  )
              )
            ) {
              logger.debug(
                s"worker[$name]:: $offloadCount offloaded operations completed. process: ${processState.process.ref}"
              ) >> processState.offloads.clear >> releaseAndNotify(processState)
            }
          )
        yield ()

      /** Worker-local notify signals are routed back to this worker's home queue. This keeps the wake-up local
        * (cache-friendly) and avoids adding contention on another worker's queue.
        */
      private def enqueueNotify(ref: ProcessRef.Unknown): F[Unit] =
        homeQueue.enqueue(createNotifySignal(ref))

      private def releaseWithOptNotify(processState: ProcessState[F]): F[Unit] =
        processState.release.flatMap {
          case true =>
            logger.debug(s"process: ${processState.process.ref} has been released. process has no pending events.")
          case false =>
            logger.debug(
              s"process: ${processState.process.ref} has been released. process has some pending events. put notification event into the signal queue."
            ) >> enqueueNotify(processState.process.ref)
        }

      private def releaseAndNotify(processState: ProcessState[F]): F[Unit] =
        logger.debug(
          s"process: ${processState.process.ref} has been released. put notification event into the signal queue"
        ) >> processState.release.flatMap(_ => enqueueNotify(processState.process.ref))

      private def runEffect(
          effect0: F[Unit],
          envelope: Envelope,
          processState: ProcessState[F],
          errorHandler: Throwable => F[Unit]
      ): F[Unit] =
        logger.debug(s"worker[$name]::runEffect. envelope: $envelope") >> effect0.handleErrorWith(errorHandler)

      private def createNotifySignal(ref: ProcessRef.Unknown): Signal =
        Signal(Envelope(ProcessRef.SchedulerRef, NotifyEvent, ref))

    private def handleError[F[_]](
        process: Process[F, ?, ?],
        envelope: Envelope,
        context: Context[F],
        interpreter: Interpreter[F],
        causeId: Long,
        cause: Throwable,
        logger: LoggerWrapper[F]
    )(using effect: Effect[F]): F[Unit] =
      envelope.event match
        case failure: Failure =>
          logger.error(s"process $process has failed to handle Failure event. send to dead-letter", cause) >>
            sendToDeadLetter(DeadLetter(failure), context, interpreter, causeId)
        case event =>
          val errorMessage = s"process $process has failed to handle event: $event"
          logger.error(errorMessage, cause) >>
            sendErrorToSender(
              envelope,
              context,
              interpreter,
              causeId,
              EventHandlingException(errorMessage, cause)
            )

    private def sendErrorToSender[F[_]](
        envelope: Envelope,
        context: Context[F],
        interpreter: Interpreter[F],
        cause: Long,
        error: Throwable
    )(using effect: Effect[F]): F[Unit] =
      send(
        SystemRef,
        Failure(envelope, error),
        context.getProcessState(envelope.sender).get,
        interpreter,
        cause
      )

    private def sendToDeadLetter[F[_]](
        deadLetter: DeadLetter,
        context: Context[F],
        interpreter: Interpreter[F],
        cause: Long
    )(using effect: Effect[F], flowOps: FlowOps[F, [x] =>> Dsl[F, x]]): F[Unit] =
      send(SystemRef, deadLetter, context.getProcessState(DeadLetterRef).get, interpreter, cause)

    private def send[F[_]](
        sender: ProcessRef.Unknown,
        event: Event,
        receiver: ProcessState[F],
        interpreter: Interpreter[F],
        cause: Long
    )(using effect: Effect[F], flowOps: FlowOps[F, [x] =>> Dsl[F, x]]): F[Unit] =
      flowOps
        .send(sender, event, receiver.process.ref.asInstanceOf[ProcessRef[Event]])
        .foldMap(interpreter.interpret(sender, receiver, cause))

    private def deliverStopEvent[F[_]](
        sender: ProcessRef.Unknown,
        processState: ProcessState[F],
        interpreter: Interpreter[F],
        cause: Long
    )(using effect: Effect[F]): F[Unit] =
      if processState.process.canHandle(Stop) then
        processState.process(Stop).foldMap(interpreter.interpret(sender, processState, cause))
      else effect.pure(())

    private def stopProcess[F[_]](
        sender: ProcessRef.Unknown,
        context: Context[F],
        receiver: ProcessRef.Unknown,
        interpreter: Interpreter[F],
        cause: Long,
        logger: LoggerWrapper[F],
        onError: (ProcessRef.Unknown, Throwable) => F[Unit]
    )(using effect: Effect[F], parallel: Parallel[F]): F[Boolean] =
      effect.suspend {
        val stopChildProcesses =
          parallel.par(
            context
              .child(receiver)
              .map(child => stopProcess(receiver, context, child, interpreter, cause, logger, onError).void)
          )

        context.getProcessState(receiver) match
          case Some(processState) =>
            processState.stop().flatMap {
              case true =>
                stopChildProcesses >>
                  processState.offloads.cancelAll >>
                  deliverStopEvent(sender, processState, interpreter, cause)
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
