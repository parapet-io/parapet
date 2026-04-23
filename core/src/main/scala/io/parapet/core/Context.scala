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

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

class Context[F[_]](
    config: Parapet.ParConfig,
    val eventStore: EventStore[F],
    val eventTransformers: EventTransformers
)(using effect: Effect[F]):
  self =>

  val devMode: Boolean = config.devMode
  val tracingEnabled: Boolean = config.tracingEnabled

  private val processes = java.util.concurrent.ConcurrentHashMap[ProcessRef, ProcessState[F]]()
  private val graph = java.util.concurrent.ConcurrentHashMap[ProcessRef, ListBuffer[ProcessRef]]()
  private val parents = java.util.concurrent.ConcurrentHashMap[ProcessRef, ProcessRef]()
  private val eventLog = EventLog()

  private var scheduler: Scheduler[F] = _

  def start(scheduler0: Scheduler[F]): F[Unit] =
    effect.delay {
      scheduler = scheduler0
    } >> createSysProcesses >> sendStartEvent(ProcessRef.SystemRef).void

  private[core] def createSysProcesses: F[Unit] =
    for
      sysProcesses <- effect.delay(List(new SystemProcess[F], new Noop[F]))
      states <- Monad.sequence(sysProcesses.map(ProcessState(_, config)))
      _ <- effect.delay(states.foreach(state => processes.put(state.process.ref, state)))
    yield ()

  def schedule(task: Task[F]): F[SubmissionResult] =
    scheduler.submit(task)

  def register(parent: ProcessRef, child: Process[F]): F[ProcessRef] =
    effect.suspend {
      if !processes.containsKey(parent) then
        effect.raiseError(UnknownProcessException(s"process cannot be registered because parent $parent doesn't exist"))
      else if parents.containsKey(child.ref) then
        effect.raiseError(new IllegalStateException(s"$child has been already registered. its parent=${parents.get(child.ref)}"))
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

  def child(parent: ProcessRef): Vector[ProcessRef] =
    graph.getOrDefault(parent, ListBuffer.empty).toVector

  def registerAndStart(parent: ProcessRef, process: Process[F]): F[SubmissionResult] =
    register(parent, process) >> sendStartEvent(process.ref)

  private def sendStartEvent(processRef: ProcessRef): F[SubmissionResult] =
    val envelope = Envelope(ProcessRef.SystemRef, Start, processRef)
    scheduler.submit(Deliver(envelope, createTrace(envelope.id)))

  def registerAll(processes0: List[Process[F]]): F[List[ProcessRef]] =
    registerAll(ProcessRef.SystemRef, processes0)

  def registerAll(parent: ProcessRef, processes0: List[Process[F]]): F[List[ProcessRef]] =
    for
      refs <- Monad.sequence(processes0.map(register(parent, _)))
      result <- Monad.sequence(refs.map(ref => sendStartEvent(ref).as(ref)))
    yield result

  def getProcesses: List[Process[F]] =
    processes.values().asScala.map(_.process).toList

  def getProcess(ref: ProcessRef): Option[Process[F]] =
    getProcessState(ref).map(_.process)

  def getProcessState(ref: ProcessRef): Option[ProcessState[F]] =
    Option(processes.get(ref))

  def interrupt(ref: ProcessRef): F[Boolean] =
    getProcessState(ref) match
      case Some(state) => state.terminate
      case None        => effect.pure(false)

  def remove(ref: ProcessRef): F[Boolean] =
    effect.delay {
      graph.computeIfPresent(parents.get(ref), (_, values) => values -= ref)
      processes.remove(ref) != null
    }

  def createTrace: ExecutionTrace =
    createTrace(UUID.randomUUID().toString)

  def createTrace(id: String): ExecutionTrace =
    if tracingEnabled then ExecutionTrace(id) else ExecutionTrace.Dummy

  def addToEventLog(envelope: Envelope): F[Unit] =
    if config.eventLogEnabled then effect.delay(eventLog.add(envelope)) else effect.pure(())

  def saveEventLog: F[Unit] =
    if config.eventLogEnabled then effect.delay(()) else effect.pure(())

object Context:
  def apply[F[_]](
      config: Parapet.ParConfig,
      eventStore: EventStore[F],
      eventTransformers: EventTransformers
  )(using effect: Effect[F]): F[Context[F]] =
    effect.delay(new Context[F](config, eventStore, eventTransformers))

  final case class BlockingOp[F[_]](blockingOperation: EffectFiber[F, Unit], done: Deferred[F, Unit])

  final class AsyncOps[F[_]](using effect: Effect[F]):
    private val signals = new AtomicReference[List[BlockingOp[F]]](Nil)

    def add(blockingOperation: EffectFiber[F, Unit], done: Deferred[F, Unit]): F[Unit] =
      effect.delay {
        signals.updateAndGet(list => list :+ BlockingOp(blockingOperation, done))
        ()
      }

    def waitForCompletion: F[Unit] =
      Monad.sequenceDiscard(signals.get().map(_.done.get))

    def clear: F[Unit] =
      effect.delay {
        signals.set(Nil)
        ()
      }

    def size: F[Int] =
      effect.pure(signals.get().size)

    def completeAll: F[Unit] =
      for
        _ <- Monad.sequenceDiscard(signals.get().map(_.blockingOperation.cancel))
        _ <- Monad.sequenceDiscard(signals.get().map(_.done.complete(()).void))
      yield ()

  final class ProcessState[F[_]](
      queue: TaskQueue[F],
      val process: Process[F],
      val terminationSignal: Deferred[F, Unit]
  )(using effect: Effect[F]):

    private val terminatedRef = new AtomicBoolean(false)
    private val stoppedRef = new AtomicBoolean(false)
    private val suspendedRef = new AtomicBoolean(false)
    private val blockingRef = new AsyncOps[F]
    private val processLock = new ProcessState.ProcessLock[F]

    def blocking: AsyncOps[F] =
      blockingRef

    def tryPut(task: Task[F]): F[Boolean] =
      queue.tryEnqueue(task)

    def tryTakeTask: F[Option[Task[F]]] =
      queue.tryDequeue

    def terminate: F[Boolean] =
      effect.delay(terminatedRef.compareAndSet(false, true)).flatMap {
        case true  => terminationSignal.complete(()).as(true)
        case false => effect.pure(false)
      }

    def stop(): F[Boolean] =
      effect.delay(stoppedRef.compareAndSet(false, true))

    def terminated: F[Boolean] =
      effect.pure(terminatedRef.get())

    def stopped: F[Boolean] =
      effect.pure(stoppedRef.get())

    def isBlocking: F[Boolean] =
      blocking.size.map(_ > 0)

    def acquired: F[Boolean] =
      processLock.acquired

    def acquire: F[Boolean] =
      processLock.acquire

    def release: F[Boolean] =
      processLock.release

    def suspended: F[Boolean] =
      effect.pure(suspendedRef.get())

    def suspend: F[Unit] =
      effect.delay {
        if !suspendedRef.compareAndSet(false, true) then
          throw new IllegalStateException(s"process[${process.ref}] is already suspended")
        ()
      }

    def resume: F[Unit] =
      effect.delay {
        if !suspendedRef.compareAndSet(true, false) then
          throw new IllegalStateException(s"process[${process.ref}] is not suspended")
        ()
      }

  object ProcessState:
    final class ProcessLock[F[_]](using effect: Effect[F]):
      private val lock = new AtomicBoolean()
      private val lockSentinel = new AtomicBoolean()

      def acquired: F[Boolean] =
        effect.delay {
          lockSentinel.set(true)
          lock.get()
        }

      def acquire: F[Boolean] =
        effect.delay(lock.compareAndSet(false, true))

      def release: F[Boolean] =
        effect.delay {
          lock.compareAndSet(true, false)
          !lockSentinel.compareAndSet(true, false)
        }

    def apply[F[_]](process: Process[F], config: Parapet.ParConfig)(using effect: Effect[F]): F[ProcessState[F]] =
      val processBufferSize =
        if process.bufferSize != -1 then process.bufferSize else config.processBufferSize

      for
        queue <-
          if processBufferSize == -1 then Queue.unbounded[F, Task[F]]()
          else Queue.bounded[F, Task[F]](processBufferSize, ChannelType.SPSC)
        terminationSignal <- Deferred[F, Unit]()
      yield new ProcessState[F](queue, process, terminationSignal)
