package io.parapet.core

import io.parapet.core.Context.*
import io.parapet.core.Events.Start
import io.parapet.core.Queue.ChannelType
import io.parapet.core.Scheduler.{Deliver, SubmissionResult, Task, TaskQueue}
import io.parapet.core.exceptions.UnknownProcessException
import io.parapet.core.processes.{Noop, SystemProcess}
import io.parapet.effect.{Deferred, Effect, EffectFiber, Monad}
import io.parapet.effect.Monad.*
import io.parapet.{Envelope, ProcessRef}

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
  * @param eventStore
  *   sink for envelopes when [[Parapet.ParConfig.eventLogEnabled]] is on.
  * @param eventTransformers
  *   per-process pipeline of [[EventTransformer]]s registered before startup.
  */
class Context[F[_]](
    config: Parapet.ParConfig,
    val eventStore: EventStore[F],
    val eventTransformers: EventTransformers
)(using effect: Effect[F]):
  self =>

  /** Convenience accessor for [[Parapet.ParConfig.devMode]]. */
  val devMode: Boolean = config.devMode

  /** Convenience accessor for [[Parapet.ParConfig.tracingEnabled]]. */
  val tracingEnabled: Boolean = config.tracingEnabled

  private val processes = java.util.concurrent.ConcurrentHashMap[ProcessRef.Unknown, ProcessState[F]]()
  private val graph     = java.util.concurrent.ConcurrentHashMap[ProcessRef.Unknown, ListBuffer[ProcessRef.Unknown]]()
  private val parents   = java.util.concurrent.ConcurrentHashMap[ProcessRef.Unknown, ProcessRef.Unknown]()
  private val eventLog  = EventLog()

  /** Global monotonic delivery sequence. */
  private val seqCounter = new AtomicLong(0L)

  /** Returns the next global delivery sequence number. */
  def nextSeq(): Long = seqCounter.incrementAndGet()

  private var scheduler: Scheduler[F] = _

  /** Binds this context to a [[Scheduler]], creates the built-in system processes, and sends the initial
    * [[Events.Start]] event. Called once during application boot.
    */
  def start(scheduler0: Scheduler[F]): F[Unit] =
    effect.delay {
      scheduler = scheduler0
    } >> createSysProcesses >> sendStartEvent(ProcessRef.SystemRef).void

  private[core] def createSysProcesses: F[Unit] =
    for
      sysProcesses <- effect.delay(List(new SystemProcess[F], new Noop[F]))
      states       <- Monad.sequence(sysProcesses.map(ProcessState(_, config)))
      _            <- effect.delay(states.foreach(state => processes.put(state.process.ref, state)))
    yield ()

  /** Submits a [[Scheduler.Task]] for execution. Equivalent to calling the scheduler directly but kept here so user
    * code can route everything through the context.
    */
  def schedule(task: Task[F]): F[SubmissionResult] =
    scheduler.submit(task)

  /** Registers `child` under `parent` in the supervision graph. The child receives a fresh [[ProcessState]] (mailbox,
    * locks) and is wired into the [[Context]] but does not automatically receive a [[Events.Start]] - see
    * [[registerAndStart]] for that.
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
      else if parents.containsKey(child.ref) then
        effect.raiseError(
          new IllegalStateException(s"$child has been already registered. its parent=${parents.get(child.ref)}")
        )
      else
        child.init(self)
        ProcessState(child, config).flatMap { state =>
          effect.delay {
            if processes.putIfAbsent(child.ref, state) != null then
              throw new RuntimeException(s"duplicated process. ref = ${child.ref}")

            parents.put(child.ref, parent)
            graph.computeIfAbsent(parent, _ => ListBuffer.empty)
            graph.computeIfPresent(parent, (_, values) => values :+ child.ref)
            child.ref
          }
        }
    }

  /** Direct children of `parent` in the supervision graph. */
  def child(parent: ProcessRef.Unknown): Vector[ProcessRef.Unknown] =
    graph.getOrDefault(parent, ListBuffer.empty).toVector

  /** Combination of [[register]] and dispatch of the initial [[Events.Start]] event. */
  def registerAndStart(parent: ProcessRef.Unknown, process: Process[F, ?, ?]): F[SubmissionResult] =
    register(parent, process) >> sendStartEvent(process.ref)

  private def sendStartEvent(processRef: ProcessRef.Unknown): F[SubmissionResult] =
    val envelope = Envelope(ProcessRef.SystemRef, Start, processRef)
    scheduler.submit(Deliver(envelope, createTrace(envelope.id)))

  /** Registers a batch of root processes (parented to [[ProcessRef.SystemRef]]) and starts each.
    */
  def registerAll(processes0: List[Process[F, ?, ?]]): F[List[ProcessRef.Unknown]] =
    registerAll(ProcessRef.SystemRef, processes0)

  /** Registers and starts each of `processes0` under `parent`. */
  def registerAll(parent: ProcessRef.Unknown, processes0: List[Process[F, ?, ?]]): F[List[ProcessRef.Unknown]] =
    for
      refs   <- Monad.sequence(processes0.map(register(parent, _)))
      result <- Monad.sequence(refs.map(ref => sendStartEvent(ref).as(ref)))
    yield result

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
      graph.computeIfPresent(parents.get(ref), (_, values) => values -= ref)
      processes.remove(ref) != null
    }

  /** Allocates a fresh [[ExecutionTrace]] (or [[ExecutionTrace.Dummy]] when tracing is disabled).
    */
  def createTrace: ExecutionTrace =
    createTrace(0L)

  /** Returns an [[ExecutionTrace]] seeded with `id` (or [[ExecutionTrace.Dummy]] when tracing is disabled).
    */
  def createTrace(id: Long): ExecutionTrace =
    if tracingEnabled then ExecutionTrace(id) else ExecutionTrace.Dummy

  /** Appends `envelope` to the in-memory [[EventLog]] when event logging is enabled; otherwise no-op.
    */
  def addToEventLog(envelope: Envelope): F[Unit] =
    if config.eventLogEnabled then effect.delay(eventLog.add(envelope)) else effect.pure(())

  /** Hook invoked at runtime shutdown to flush the event log. The current implementation is a placeholder; future
    * versions may persist to disk.
    */
  def saveEventLog: F[Unit] =
    if config.eventLogEnabled then effect.delay(()) else effect.pure(())

/** Factory and inner types supporting [[Context]]. */
object Context:
  /** Allocates a new [[Context]] in `F`. */
  def apply[F[_]](
      config: Parapet.ParConfig,
      eventStore: EventStore[F],
      eventTransformers: EventTransformers
  )(using effect: Effect[F]): F[Context[F]] =
    effect.delay(new Context[F](config, eventStore, eventTransformers))

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
      val terminationSignal: Deferred[F, Unit]
  )(using effect: Effect[F]):

    private val terminatedRef  = new AtomicBoolean(false)
    private val stoppedRef     = new AtomicBoolean(false)
    private val suspendedRef   = new AtomicBoolean(false)
    private val offloadTracker = new OffloadTracker[F]
    private val processLock    = new ProcessState.ProcessLock[F]

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
    def apply[F[_]](process: Process[F, ?, ?], config: Parapet.ParConfig)(using effect: Effect[F]): F[ProcessState[F]] =
      val processBufferSize =
        if process.bufferSize != -1 then process.bufferSize else config.processBufferSize

      for
        queue <-
          if processBufferSize == -1 then Queue.unbounded[F, Task[F]]()
          else Queue.bounded[F, Task[F]](processBufferSize, ChannelType.SPSC)
        terminationSignal <- Deferred[F, Unit]()
      yield new ProcessState[F](queue, process, terminationSignal)
