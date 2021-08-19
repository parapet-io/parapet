package io.parapet.core

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, ContextShift, Fiber}
import cats.implicits._
import io.parapet.core.Context._
import io.parapet.core.Events.Start
import io.parapet.core.Queue.ChannelType
import io.parapet.core.Scheduler.{Deliver, SubmissionResult, Task, TaskQueue}
import io.parapet.core.exceptions.UnknownProcessException
import io.parapet.core.processes.{BlackHole, SystemProcess}

import java.util.UUID
import scala.jdk.CollectionConverters._

class Context[F[_]: Concurrent: ContextShift](config: Parapet.ParConfig, val eventStore: EventStore[F]) {

  val devMode: Boolean = config.devMode

  val tracingEnabled: Boolean = config.tracingEnabled

  private val ct = implicitly[Concurrent[F]]

  private val processes = new java.util.concurrent.ConcurrentHashMap[ProcessRef, ProcessState[F]]()

  private val graph = new java.util.concurrent.ConcurrentHashMap[ProcessRef, Vector[ProcessRef]]

  private val eventLog = new EventLog()

  private var _scheduler: Scheduler[F] = _

  def start(scheduler: Scheduler[F]): F[Unit] =
    ct.delay {
      _scheduler = scheduler
    } >> createSysProcesses >> sendStartEvent(ProcessRef.SystemRef).void

  private[core] def createSysProcesses: F[Unit] = {
    for {
      sysProcesses <- ct.delay(List(new SystemProcess[F](), new BlackHole[F]))
      states <- sysProcesses.map(p => ProcessState(p, config)).sequence
      _ <- ct.delay(states.foreach(s => processes.put(s.process.ref, s)))
    } yield ()
  }

  def schedule(task: Task[F]): F[SubmissionResult] = _scheduler.submit(task)

  def register(parent: ProcessRef, process: Process[F]): F[ProcessRef] =
    if (!processes.containsKey(parent)) {
      ct.raiseError(
        UnknownProcessException(s"process cannot be registered because parent process with id=$parent doesn't exist"),
      )
    } else {
      ProcessState(process, config).flatMap { s =>
        if (processes.putIfAbsent(process.ref, s) != null)
          ct.raiseError(new RuntimeException(s"duplicated process. ref = ${process.ref}"))
        else {
          graph.computeIfAbsent(parent, _ => Vector())
          graph.computeIfPresent(parent, (_, v) => v :+ process.ref)
          ct.pure(process.ref)
        }
      }
    }

  def child(parent: ProcessRef): Vector[ProcessRef] = graph.getOrDefault(parent, Vector.empty)

  def registerAndStart(parent: ProcessRef, process: Process[F]): F[SubmissionResult] =
    register(parent, process) >> sendStartEvent(process.ref)

  private def sendStartEvent(processRef: ProcessRef): F[SubmissionResult] = {
    val e = Envelope(ProcessRef.SystemRef, Start, processRef)
    _scheduler.submit(Deliver(e, createTrace(e.id)))
  }

  def registerAll(processes: List[Process[F]]): F[List[ProcessRef]] =
    registerAll(ProcessRef.SystemRef, processes)

  def registerAll(parent: ProcessRef, processes: List[Process[F]]): F[List[ProcessRef]] =
    for {
      refs <- processes.map(p => register(parent, p)).sequence
      res <- refs.map(ref => sendStartEvent(ref) >> ct.pure(ref)).sequence
    } yield res

  def getProcesses: List[Process[F]] = processes.values().asScala.map(_.process).toList

  def getProcess(ref: ProcessRef): Option[Process[F]] =
    getProcessState(ref).map(_.process)

  def getProcessState(ref: ProcessRef): Option[ProcessState[F]] =
    Option(processes.get(ref))

  def interrupt(pRef: ProcessRef): F[Boolean] =
    getProcessState(pRef) match {
      case Some(s) => s.terminate
      case None => ct.pure(false)
    }

  def remove(pRef: ProcessRef): F[Option[Process[F]]] =
    ct.delay(Option(processes.remove(pRef)).map(_.process))

  def createTrace: ExecutionTrace =
    createTrace(UUID.randomUUID().toString)

  def createTrace(id: String): ExecutionTrace =
    if (tracingEnabled) ExecutionTrace(id)
    else ExecutionTrace.Dummy

  def addToEventLog(e: Envelope): F[Unit] =
    if (config.eventLogEnabled) ct.delay(eventLog.add(e))
    else ct.unit

  def saveEventLog: F[Unit] =
    if (config.eventLogEnabled) {
      ct.delay {
        eventLog.close()
        println(EventLog.Cytoscape.toJson(eventLog)) // todo store to file
      }
    } else {
      ct.unit
    }

}

object Context {

  def apply[F[_]: Concurrent: ContextShift](config: Parapet.ParConfig, eventLog: EventStore[F]): F[Context[F]] =
    implicitly[Concurrent[F]].delay(new Context[F](config, eventLog))

  /** Represents a blocking operation executed by a process.
    *
    * @param blockingOperation the forked blocking operation. Also this fiber is used to cancel the operation.
    * @param done the promise that will be completed once the blocking operation has finished.
    * @tparam F effect type
    */
  case class BlockingOp[F[_]: Concurrent](blockingOperation: Fiber[F, Unit], done: Deferred[F, Unit])

  class AsyncOps[F[_]: Concurrent] {

    private val signals = new AtomicReference[List[BlockingOp[F]]](List.empty)
    private val ct = implicitly[Concurrent[F]]

    def add(blockingOperation: Fiber[F, Unit], done: Deferred[F, Unit]): F[Unit] =
      ct.delay(signals.updateAndGet(l => l :+ BlockingOp(blockingOperation, done)))

    def waitForCompletion: F[Unit] =
      signals.get().map(o => o.done.get).sequence_

    def clear: F[Unit] =
      ct.delay(signals.set(List.empty))

    def size: F[Int] = ct.pure(signals.get.size)

    /** Completes all blocking operations. Any running blocking operations will be canceled via [[Fiber.cancel]].
      * @return
      */
    def completeAll: F[Unit] = for {
      _ <- signals.get.map(_.blockingOperation.cancel).sequence_
      _ <- signals.get.map(_.done.complete(())).sequence_
    } yield ()
  }

  /** Process state used to manage a process lifecycle, e.g.: access control,
    * locking mechanism, termination, blocking operations management.
    *
    * @param queue the tasks queue
    * @param process the process
    * @param terminationSignal completed once the process terminated via [[terminate]].
    *                          Used to cancel all outgoing computations related to this process.
    * @tparam [[F]] effect
    */
  class ProcessState[F[_]: Concurrent](
      queue: TaskQueue[F],
      val process: Process[F],
      val terminationSignal: Deferred[F, Unit],
  ) {

    import ProcessState._

    private val ct = implicitly[Concurrent[F]]

    private[this] val _terminated: AtomicBoolean = new AtomicBoolean(false)
    private[this] val _stopped: AtomicBoolean = new AtomicBoolean(false)
    private[this] val _suspended: AtomicBoolean = new AtomicBoolean(false)
    private[this] val _blocking: AsyncOps[F] = new AsyncOps()
    private[this] val pLock = new ProcessLock[F]()

    def blocking: AsyncOps[F] = _blocking

    def tryPut(t: Task[F]): F[Boolean] =
      queue.tryEnqueue(t)

    def tryTakeTask: F[Option[Task[F]]] = queue.tryDequeue

    /** Changes the process state to terminated.
      *
      * @return true if the operation was successful
      */
    def terminate: F[Boolean] =
      ct.delay(_terminated.compareAndSet(false, true)).flatMap {
        case true => terminationSignal.complete(()).map(_ => true)
        case false => ct.pure(false)
      }

    def stop(): F[Boolean] = ct.delay(_stopped.compareAndSet(false, true))

    def terminated: F[Boolean] = ct.pure(_terminated.get())

    def stopped: F[Boolean] = ct.pure(_stopped.get())

    def isBlocking: F[Boolean] = blocking.size.map(_ > 0)

    def acquired: F[Boolean] = pLock.acquired

    def acquire: F[Boolean] = pLock.acquire

    def release: F[Boolean] = pLock.release

    /** returns `true` if this process performing some blocking operations, other `false`
      */
    def suspended: F[Boolean] = ct.pure(_suspended.get())

    def suspend: F[Unit] = ct.delay {
      if (!_suspended.compareAndSet(false, true)) {
        throw new IllegalStateException(s"process[${process.ref}] is already suspended")
      }
    }

    def resume: F[Unit] = ct.delay {
      if (!_suspended.compareAndSet(true, false)) {
        throw new IllegalStateException(s"process[${process.ref}] is not suspended")
      }
    }

  }

  object ProcessState {
    def apply[F[_]: Concurrent: ContextShift](process: Process[F], config: Parapet.ParConfig): F[ProcessState[F]] = {
      val processBufferSize = if (process.bufferSize != -1) process.bufferSize else config.processBufferSize
      for {
        queue <-
          if (processBufferSize == -1) Queue.unbounded[F, Task[F]]()
          else Queue.bounded[F, Task[F]](processBufferSize, ChannelType.SPSC)
        terminationSignal <- Deferred[F, Unit]
      } yield new ProcessState[F](queue, process, terminationSignal)
    }

    class ProcessLock[F[_]: Concurrent] {
      private val lock = new AtomicBoolean()

      /** used to track whenever the lock is checked via [[acquired]].
        */
      private val lockSentinel = new AtomicBoolean()
      private val ct = implicitly[Concurrent[F]]

      def acquired: F[Boolean] = ct.delay {
        lockSentinel.set(true)
        lock.get()
      }

      def acquire: F[Boolean] = ct.delay {
        lock.compareAndSet(false, true)
      }

      /** Releases the lock.
        * @return true - if [[acquired]] was called while the lock was acquired, otherwise false.
        */
      def release: F[Boolean] =
        ct.delay {
          lock.compareAndSet(true, false)
          !lockSentinel.compareAndSet(true, false)
        }
    }

  }

}
