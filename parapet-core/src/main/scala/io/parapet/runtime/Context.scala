package io.parapet.runtime

import Context.*
import io.parapet.core.DslInterpreter.Interpreter
import io.parapet.core.Events.{Initialize, Registered, Start}
import io.parapet.core.Queue.ChannelType
import Scheduler.{Deliver, SubmissionResult, Task, TaskQueue}
import io.parapet.core.exceptions.UnknownProcessException
import io.parapet.core.processes.{Noop, SystemProcess}
import io.parapet.core.{Clock, Events, Parapet, Process, Queue}
import io.parapet.effect.Monad.*
import io.parapet.effect.{Deferred, Effect, EffectFiber, Monad}
import io.parapet.journal.*
import io.parapet.snapshot.{SnapshotManager, SnapshotStorage, Snapshotable}
import io.parapet.{Event, ProcessRef}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

/** The runtime state for a single parapet application: registry of running processes, supervision graph, event log, and
  * a handle to the [[Scheduler]] that drives them.
  *
  * Every process registered through [[register]] / [[registerAll]] gets a [[ProcessState]] holding its mailbox,
  * lifecycle flags, and synchronization primitives. The `Context` is thread-safe; [[Scheduler.Worker]]s and user code
  * may interact with it concurrently.
  *
  * Users normally do not construct a `Context` directly - [[io.parapet.ParApp.run]] does so during boot. Application
  * code receives one indirectly through `Process.context`.
  *
  * @param config
  *   runtime configuration in effect for this context.
  * @param eventTransformers
  *   per-process pipeline of [[EventTransformer]]s registered before startup.
  */
class Context[F[_]](
    config: Parapet.ParConfig,
    val eventTransformers: EventTransformers,
    private[parapet] val snapshotManager: Option[SnapshotManager[F]],
    private[parapet] val recorder: Option[DeliveryRecorder[F]],
    private[parapet] val codecRegistry: EventCodecRegistry
)(using effect: Effect[F]):
  self =>

  /** Convenience accessor for [[Parapet.ParConfig.devMode]]. */
  val devMode: Boolean = config.devMode

  /** Enables/Disables snapshotting. */
  val snapshotEnabled: Boolean = snapshotManager.isDefined

  /** Trigger snapshot every-N. */
  val maxEventsPerSnapshot: Int = config.snapshot.maxEventsPerSnapshot

  /** Wall-clock cadence in millis. `0` disables the time trigger. */
  val maxSnapshotIntervalMillis: Long = config.snapshot.maxSnapshotIntervalMillis

  private val clock: Clock = Clock()

  /** Stops snapshotting. No-op when snapshotting is off. */
  def stopSnapshotting: F[Unit] =
    snapshotManager.fold(effect.pure(()))(_.close)

  /** Enqueues an asynchronous snapshot of `process` as of delivery `seq`. Returns `true` if it was enqueued, otherwise -
    * `false`.
    */
  def snapshotAsync(ref: ProcessRef.Unknown, process: Snapshotable, seq: Long): F[Boolean] =
    snapshotManager.fold(effect.pure(false))(_.createAsync(ref, process, seq))

  /** Whether the delivery journal is recording. */
  val journalEnabled: Boolean = recorder.isDefined

  /** When true, a delivery whose event has no codec fails loud instead of being skipped. */
  val requireEventCodec: Boolean = config.journal.requireCodec

  /** The codec for `event`, or `None` when its type is not registered */
  def codecFor(event: Event): Option[EventCodec] = codecRegistry.codecFor(event)

  /** The codec that recorded entries under `tag`, or `None` when the tag is unknown */
  def codecForTag(tag: String): Option[EventCodec] = codecRegistry.codecForTag(tag)

  private val bootModeRef = new AtomicReference[BootMode](BootMode.Live)

  /** Current boot mode; [[BootMode.Replaying]] while recovery re-folds recorded history, else [[BootMode.Live]]. */
  def bootMode: BootMode = bootModeRef.get

  /** True while replaying recorded history */
  def replaying: Boolean = bootMode == BootMode.Replaying

  private[parapet] def bootMode_=(mode: BootMode): Unit = bootModeRef.set(mode)

  /** Records `draft` in the delivery journal and returns its assigned delivery `seq`. Only valid when journalling is on
    * (the caller reaches this only for a journalable delivery).
    */
  def admit(draft: JournalDraft): F[Long] =
    recorder.fold(effect.raiseError[Long](new IllegalStateException("journal is not enabled")))(_.admit(draft))

  /** Stops the journal, publishing any buffered tail. No-op when the journal is off. */
  def stopJournal: F[Unit] =
    recorder.fold(effect.pure(()))(_.close())

  /** Recorded deliveries with `seq > afterSeq` in ascending order, or empty when the journal is off. */
  def readJournal(afterSeq: Long): F[Vector[JournalEntry]] =
    recorder.fold(effect.pure(Vector.empty[JournalEntry]))(_.read(afterSeq))

  private val processes = java.util.concurrent.ConcurrentHashMap[ProcessRef.Unknown, ProcessState[F]]()
  private val graph     = java.util.concurrent.ConcurrentHashMap[ProcessRef.Unknown, ListBuffer[ProcessRef.Unknown]]()
  private val parents   = java.util.concurrent.ConcurrentHashMap[ProcessRef.Unknown, ProcessRef.Unknown]()

  // Global monotonic delivery sequence. When the journal is on, the recorder owns it
  private val seqCounter = new AtomicLong(0L)

  /** Returns the next global delivery position for a snapshot-tracked but unjournalled delivery. */
  def nextSeq(): F[Long] =
    recorder.fold(effect.delay(seqCounter.incrementAndGet()))(_.advanceSequence())

  private[parapet] def continueSeqAfter(seq: Long): Unit =
    recorder match
      case Some(r) => r.continueAfter(seq)
      case None    => seqCounter.updateAndGet(current => math.max(current, seq)); ()

  private var _scheduler: Scheduler[F] = _

  /** The scheduler bound to this context via [[bind]]; `null` until then.
    */
  private[runtime] def scheduler: Scheduler[F] = _scheduler

  /** Binds `scheduler` to this context and creates the built-in system processes. Must run before [[Scheduler.start]].
    */
  def bind(scheduler: Scheduler[F]): F[Unit] =
    effect.delay {
      _scheduler = scheduler
    } >> createSysProcesses

  private[runtime] def createSysProcesses: F[Unit] =
    for
      sysProcesses <- effect.delay(List(new SystemProcess[F], new Noop[F]))
      states       <- Monad.sequence(sysProcesses.map(ProcessState(_, config, clock)))
      _            <- effect.delay(states.foreach(state => processes.put(state.process.ref, state)))
    yield ()

  /** Submits a [[Scheduler.Task]] for execution. Equivalent to calling the scheduler directly but kept here so user
    * code can route everything through the context.
    */
  def schedule(task: Task[F]): F[SubmissionResult] =
    scheduler.submit(task)

  /** Registers `child` under `parent` in the supervision graph. The child receives a fresh [[ProcessState]] (mailbox,
    * locks) and is wired into the [[Context]] but does not automatically receive lifecycle events - see
    * [[registerAndStart]].
    *
    * @return
    *   the child's [[ProcessRef]], which equals `child.ref`.
    * @throws io.parapet.core.exceptions.UnknownProcessException
    *   if `parent` is not registered.
    */
  def register(parent: ProcessRef.Unknown, child: Process[F, ?, ?]): F[ProcessRef.Unknown] =
    effect.suspend {
      if !processes.containsKey(parent) then
        effect.raiseError(UnknownProcessException(s"process cannot be registered because parent $parent doesn't exist"))
      else
        child.init(self)
        ProcessState(child, config, clock).flatMap { state =>
          effect
            .delay {
              if processes.putIfAbsent(child.ref, state) != null then
                throw new IllegalStateException(s"duplicated process. ref = ${child.ref}")
            }
            .flatMap { _ =>
              recordRegistration(parent, child.ref)
                .handleErrorWith { error =>
                  effect.delay(processes.remove(child.ref, state)).flatMap(_ => effect.raiseError(error))
                } >> effect.delay {
                parents.put(child.ref, parent)
                graph.computeIfAbsent(parent, _ => ListBuffer.empty)
                graph.computeIfPresent(parent, (_, values) => values :+ child.ref)
                child.ref
              }
            }
        }
    }

  private def recordRegistration(parent: ProcessRef.Unknown, child: ProcessRef.Unknown): F[Unit] =
    if replaying then effect.pure(())
    else
      recorder match
        case None    => effect.pure(())
        case Some(_) =>
          effect.delay(Envelope(ProcessRef.SystemRef, Registered(child), parent)).flatMap { envelope =>
            admit(JournalDraft(envelope.id, envelope.sender, envelope.receiver, envelope.cause, envelope.event)).void
          }

  /** Direct children of `parent` in the supervision graph. */
  def child(parent: ProcessRef.Unknown): Vector[ProcessRef.Unknown] =
    graph.getOrDefault(parent, ListBuffer.empty).toVector

  /** Registers a child and schedules [[Events.Initialize]] followed by [[Events.Start]]. */
  def registerAndStart(parent: ProcessRef.Unknown, process: Process[F, ?, ?]): F[SubmissionResult] =
    register(parent, process) >> sendLifecycleEvent(process.ref, Initialize) >> sendStartEvent(process.ref)

  private[parapet] def sendStartEvent(processRef: ProcessRef.Unknown): F[SubmissionResult] =
    sendLifecycleEvent(processRef, Start)

  private[parapet] def sendLifecycleEvent(
      processRef: ProcessRef.Unknown,
      event: Events.SystemEvent
  ): F[SubmissionResult] =
    scheduler.submit(Deliver(Envelope(ProcessRef.SystemRef, event, processRef)))

  /** Registers a batch of root processes and schedules [[Events.Initialize]] followed by [[Events.Start]]. */
  def registerAll(processes0: List[Process[F, ?, ?]]): F[List[ProcessRef.Unknown]] =
    registerAll(ProcessRef.SystemRef, processes0)

  /** Registers and activates a batch of processes under `parent`. */
  def registerAll(parent: ProcessRef.Unknown, processes0: List[Process[F, ?, ?]]): F[List[ProcessRef.Unknown]] =
    for
      refs <- Monad.sequence(processes0.map(register(parent, _)))
      _    <- Monad.sequence(processes0.map(p => sendLifecycleEvent(p.ref, Initialize) >> sendStartEvent(p.ref)))
    yield refs

  /** Restores and replays the application, then schedules [[Events.Start]] for the live phase. */
  private[parapet] def boot(processes0: List[Process[F, ?, ?]], interpreter: Interpreter[F]): F[Unit] =
    val recovery = new Recovery(self, interpreter)
    effect.delay {
      bootMode = BootMode.Replaying
    } >>
      recovery.seedSequencers() >>
      recovery.boot(processes0) >>
      effect.delay {
        bootMode = BootMode.Live
      } >> Monad.sequence(getProcesses.map(process => sendStartEvent(process.ref))).void

  /** Highest delivery `seq` recorded in the journal, or `None`; seeds the delivery counter at boot. */
  private[parapet] def journalMaxSeq: F[Option[Long]] = recorder.fold(effect.pure(Option.empty[Long]))(_.maxSeq)

  /** Highest envelope id the journal refers to, or `None` */
  private[parapet] def journalMaxEnvelopeId: F[Option[Long]] =
    recorder.fold(effect.pure(Option.empty[Long]))(_.maxEnvelopeId)

  /** Snapshot of every [[Process]] currently registered (system + user). */
  def getProcesses: List[Process[F, ?, ?]] =
    processes.values().asScala.map(_.process).toList

  /** Looks up a process by ref. */
  def getProcess(ref: ProcessRef.Unknown): Option[Process[F, ?, ?]] =
    getProcessState(ref).map(_.process)

  /** Looks up the runtime state (mailbox + lifecycle flags) of a process. */
  def getProcessState(ref: ProcessRef.Unknown): Option[ProcessState[F]] =
    Option(processes.get(ref))

  /** Marks the process as terminated, completing its termination signal. Returns `true` if this call was the one that
    * flipped the flag (idempotent across concurrent callers).
    */
  def interrupt(ref: ProcessRef.Unknown): F[Boolean] =
    getProcessState(ref) match
      case Some(state) => state.terminate
      case None        => effect.pure(false)

  /** Removes the process from the registry and detaches it from its parent in the supervision graph. Returns `true` if
    * a process was actually removed.
    */
  def remove(ref: ProcessRef.Unknown): F[Boolean] =
    effect.delay {
      val parent = parents.remove(ref)
      if parent != null then graph.computeIfPresent(parent, (_, values) => values -= ref)
      processes.remove(ref) != null
    }

/** Factory and inner types supporting [[Context]]. */
object Context:
  private val NanosPerMilli = 1_000_000L

  /** Allocates a new [[Context]] in `F`. `config.snapshot.enabled` / `config.journal.enabled` are the switches: when a
    * feature is on its storage must be provided, when off the storage is ignored.
    */
  def apply[F[_]](
      config: Parapet.ParConfig,
      eventTransformers: EventTransformers,
      snapshotStorage: Option[SnapshotStorage[F]] = None,
      journalStorage: Option[JournalStore[F]] = None,
      codecRegistry: EventCodecRegistry = EventCodecRegistry.empty
  )(using effect: Effect[F]): F[Context[F]] =
    for
      snapshots <- buildWithStorage(config.snapshot.enabled, snapshotStorage, "snapshotting")(
        SnapshotManager[F](_, Clock(), config.snapshot.queueCapacity)
      )
      recorder <- buildWithStorage(config.journal.enabled, journalStorage, "journal")(store =>
        effect.pure(DeliveryRecorder.fresh(store, codecRegistry, config.journal))
      )
    yield new Context[F](config, eventTransformers, snapshots, recorder, codecRegistry)

  private def buildWithStorage[F[_], S, M](enabled: Boolean, storage: Option[S], name: String)(
      build: S => F[M]
  )(using effect: Effect[F]): F[Option[M]] =
    if !enabled then effect.pure(None)
    else
      storage match
        case Some(s) => build(s).map(Some(_))
        case None    => effect.raiseError(new IllegalStateException(s"$name is enabled but no storage was provided"))

  /** Pairs an offloaded operation with the [[Deferred]] that records how it completed.
    */
  final private case class OffloadHandle[F[_]](
      fiber: EffectFiber[F, Unit],
      completion: Deferred[F, Either[Throwable, Unit]]
  )

  /** Bookkeeping for offloaded operations spawned by a single process.
    *
    * The scheduler tracks them so it can wait for completion before releasing the process lock.
    */
  final class OffloadTracker[F[_]](using effect: Effect[F]):
    private val signals = new AtomicReference[List[OffloadHandle[F]]](Nil)

    /** Add offloaded operation to tracking.
      */
    def add(fiber: EffectFiber[F, Unit], completion: Deferred[F, Either[Throwable, Unit]]): F[Unit] =
      effect.delay {
        signals.updateAndGet(list => list :+ OffloadHandle(fiber, completion))
        ()
      }

    /** Suspends until every recorded offloaded operation has signalled completion, then re-raises the first failure if
      * any offload op failed.
      */
    def waitForCompletion: F[Unit] =
      Monad.sequence(signals.get().map(_.completion.get)).flatMap { outcomes =>
        outcomes.collectFirst { case Left(error) => error } match
          case Some(error) => effect.raiseError(error)
          case None        => effect.pure(())
      }

    /** Drops all bookkeeping; the scheduler clears after waiting. */
    def clear: F[Unit] =
      effect.delay {
        signals.set(Nil)
        ()
      }

    /** Number of currently-tracked offloaded operations. */
    def size: F[Int] =
      effect.pure(signals.get().size)

    /** Cancels every tracked offload operation.
      */
    def cancelAll: F[Unit] =
      for
        _ <- Monad.sequenceDiscard(signals.get().map(_.fiber.cancel))
        _ <- Monad.sequenceDiscard(signals.get().map(_.completion.complete(Right(())).void))
      yield ()

  /** Per-process runtime state held by the [[Context]].
    *
    * Wraps the mailbox `queue`, the user-defined [[Process]] instance, and the lifecycle flags ([[terminate]] /
    * [[stop]] / [[suspend]]) consulted by the scheduler.
    *
    * @param queue
    *   the task mailbox dedicated to this process.
    * @param process
    *   the user-defined behavior.
    * @param terminationSignal
    *   fires once when the process is fully torn down; awaited by supervision logic and external callers.
    */
  final class ProcessState[F[_]](
      queue: TaskQueue[F],
      val process: Process[F, ?, ?],
      val terminationSignal: Deferred[F, Unit],
      clock: Clock
  )(using effect: Effect[F]):

    private val terminatedRef  = new AtomicBoolean(false)
    private val stoppedRef     = new AtomicBoolean(false)
    private val suspendedRef   = new AtomicBoolean(false)
    private val offloadTracker = new OffloadTracker[F]
    private val processLock    = new ProcessState.ProcessLock[F]

    /** Snapshot-cadence bookkeeping.
      */
    val checkpoints: CheckpointTracker = new CheckpointTracker(clock)

    /** Whether this process opts into snapshotting. */
    val snapshotable: Boolean = process.isInstanceOf[Snapshotable]

    /** Bookkeeping for offloaded operations spawned by this process. */
    def offloads: OffloadTracker[F] =
      offloadTracker

    /** Non-blocking enqueue; returns `false` when the mailbox is full. */
    def tryPut(task: Task[F]): F[Boolean] =
      queue.tryEnqueue(task)

    /** Non-blocking dequeue; returns `None` when the mailbox is empty. */
    def tryTakeTask: F[Option[Task[F]]] =
      queue.tryDequeue

    /** Marks the process as terminated and fires [[terminationSignal]]. Idempotent - returns `true` only on the call
      * that first flipped the flag.
      */
    def terminate: F[Boolean] =
      effect.delay(terminatedRef.compareAndSet(false, true)).flatMap {
        case true  => terminationSignal.complete(()).as(true)
        case false => effect.pure(false)
      }

    /** Sets the "stopped" flag (graceful shutdown initiated). Idempotent. */
    def stop(): F[Boolean] =
      effect.delay(stoppedRef.compareAndSet(false, true))

    /** True once [[terminate]] has run. */
    def terminated: F[Boolean] =
      effect.pure(terminatedRef.get())

    /** True once [[stop]] has run. */
    def stopped: F[Boolean] =
      effect.pure(stoppedRef.get())

    /** True if the process has any in-flight offloaded operations. */
    def hasOffloads: F[Boolean] =
      offloads.size.map(_ > 0)

    /** True if a worker currently holds the per-process lock. */
    def acquired: F[Boolean] =
      processLock.acquired

    /** Tries to acquire the per-process lock; returns `true` on success. The scheduler uses this to enforce the
      * per-process serialization guarantee.
      */
    def acquire: F[Boolean] =
      processLock.acquire

    /** Releases the per-process lock previously acquired via [[acquire]]. */
    def release: F[Boolean] =
      processLock.release

    /** True if the process is currently suspended. */
    def suspended: F[Boolean] =
      effect.pure(suspendedRef.get())

    /** Pauses dispatch for this process. Throws if already suspended. */
    def suspend: F[Unit] =
      effect.delay {
        if !suspendedRef.compareAndSet(false, true) then
          throw new IllegalStateException(s"process[${process.ref}] is already suspended")
        ()
      }

    /** Resumes a previously suspended process. Throws if not suspended. */
    def resume: F[Unit] =
      effect.delay {
        if !suspendedRef.compareAndSet(true, false) then
          throw new IllegalStateException(s"process[${process.ref}] is not suspended")
        ()
      }

  /** Per-process snapshot-cadence counters that drive snapshots creation speed.
    *
    * Not thread-safe.
    */
  final class CheckpointTracker(clock: Clock):
    private var eventsSinceSnapshot = 0
    private var lastSeq             = 0L
    private var lastSnapshotNanos   = Long.MinValue

    /** Records that the delivery at `seq` was consumed. The first delivery anchors the time-cadence window, so the
      * earliest time-triggered snapshot is one interval after the process first did work.
      */
    def onDelivered(seq: Long): Unit =
      if seq < lastSeq then throw IllegalStateException(s"seq=$seq < lastSeq=$lastSeq")
      eventsSinceSnapshot += 1
      lastSeq = seq
      if lastSnapshotNanos == Long.MinValue then lastSnapshotNanos = clock.nanoTime

    /** Resets the cadence stats after a snapshot was taken. */
    def onSnapshot(): Unit =
      eventsSinceSnapshot = 0
      lastSnapshotNanos = clock.nanoTime

    /** True when at least one delivery has been consumed since the last snapshot. */
    def dirty: Boolean =
      eventsSinceSnapshot > 0

    /** True when a snapshot is due: either the `maxEvents` ceiling is reached, or - when `maxIntervalMillis > 0` - that
      * much wall-clock time has elapsed since the last snapshot while state is dirty.
      */
    def due(maxEvents: Int, maxIntervalMillis: Long): Boolean =
      eventsSinceSnapshot >= maxEvents ||
        (maxIntervalMillis > 0 && dirty && clock.nanoTime - lastSnapshotNanos >= maxIntervalMillis * NanosPerMilli)

    /** Position of the most recent consumed delivery - the seq a snapshot taken now corresponds to. */
    def lastDeliveredSeq: Long =
      lastSeq

  /** Helpers and the lock primitive used by [[ProcessState]]. */
  object ProcessState:
    /** Cooperative lock guarding "exactly one worker handling at a time" for a process.
      *
      * The `lockSentinel` flag distinguishes between a release that races with a fresh acquire (in which case we must
      * re-notify) and a clean release. The scheduler relies on this signal to decide whether to re-enqueue a
      * notification.
      */
    final class ProcessLock[F[_]](using effect: Effect[F]):
      private val lock         = new AtomicBoolean()
      private val lockSentinel = new AtomicBoolean()

      /** Sets the sentinel and reads the lock state in a single step. The sentinel signals that someone consulted the
        * lock between an acquire and a release.
        */
      def acquired: F[Boolean] =
        effect.delay {
          lockSentinel.set(true)
          lock.get()
        }

      /** Tries to flip the lock from `false → true`. Returns `true` if we won. */
      def acquire: F[Boolean] =
        effect.delay(lock.compareAndSet(false, true))

      /** Releases the lock and clears the sentinel. Returns `true` if no concurrent `acquired`-style poll observed the
        * lock since it was acquired (i.e. it's safe to skip a re-notify).
        */
      def release: F[Boolean] =
        effect.delay {
          lock.compareAndSet(true, false)
          !lockSentinel.compareAndSet(true, false)
        }

    /** Builds a fresh [[ProcessState]], honoring the process's overridden mailbox size or falling back to the global
      * default.
      */
    def apply[F[_]](process: Process[F, ?, ?], config: Parapet.ParConfig, clock: Clock)(using
        effect: Effect[F]
    ): F[ProcessState[F]] =
      val processBufferSize =
        if process.bufferSize != -1 then process.bufferSize else config.processBufferSize

      for
        queue <-
          if processBufferSize == -1 then Queue.unbounded[F, Task[F]]()
          else Queue.bounded[F, Task[F]](processBufferSize, ChannelType.SPSC)
        terminationSignal <- Deferred[F, Unit]()
      yield new ProcessState[F](queue, process, terminationSignal, clock)
