package io.parapet.core

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, ContextShift, Fiber}
import cats.implicits._
import io.parapet.core.Context._
import io.parapet.core.Event.{Envelope, Start}
import io.parapet.core.Queue.ChannelType
import io.parapet.core.Scheduler.{Deliver, SubmissionResult, Task, TaskQueue}
import io.parapet.core.exceptions.UnknownProcessException
import io.parapet.core.processes.SystemProcess

import scala.collection.JavaConverters._

class Context[F[_] : Concurrent : ContextShift](
                                                 config: Parapet.ParConfig,
                                                 val eventLog: EventLog[F]) {


  private val ct = implicitly[Concurrent[F]]

  private val processes = new java.util.concurrent.ConcurrentHashMap[ProcessRef, ProcessState[F]]()

  private val graph = new java.util.concurrent.ConcurrentHashMap[ProcessRef, Vector[ProcessRef]]

  private var _scheduler: Scheduler[F] = _

  def start(scheduler: Scheduler[F]): F[Unit] = {
    ct.delay(_scheduler = scheduler) >>
      ct.delay(new SystemProcess[F]()).flatMap { sysProcess =>
        ProcessState(sysProcess, config).flatMap { s =>
          ct.delay(processes.put(sysProcess.ref, s)) >> sendStartEvent(sysProcess.ref).void
        }
      }
  }

  def schedule(task: Task[F]): F[SubmissionResult] = _scheduler.submit(task)

  def register(parent: ProcessRef, process: Process[F]): F[ProcessRef] = {
    if (!processes.containsKey(parent)) {
      ct.raiseError(UnknownProcessException(
        s"process cannot be registered because parent process with id=$parent doesn't exist"))
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
  }

  def child(parent: ProcessRef): Vector[ProcessRef] = graph.getOrDefault(parent, Vector.empty)

  def registerAndStart(parent: ProcessRef, process: Process[F]): F[SubmissionResult] = {
    register(parent, process) >> sendStartEvent(process.ref)
  }

  private def sendStartEvent(processRef: ProcessRef): F[SubmissionResult] = {
    _scheduler.submit(Deliver(Envelope(ProcessRef.SystemRef, Start, processRef)))
  }

  def registerAll(parent: ProcessRef, processes: List[Process[F]]): F[List[ProcessRef]] = {

    for {
      refs <- processes.map(p => register(parent, p)).sequence
      res <- refs.map(ref => sendStartEvent(ref) >> ct.pure(ref)).sequence
    } yield res

  }

  def getProcesses: List[Process[F]] = processes.values().asScala.map(_.process).toList

  def getProcess(ref: ProcessRef): Option[Process[F]] = {
    getProcessState(ref).map(_.process)
  }

  def getProcessState(ref: ProcessRef): Option[ProcessState[F]] = {
    Option(processes.get(ref))
  }

  def interrupt(pRef: ProcessRef): F[Boolean] = {
    getProcessState(pRef) match {
      case Some(s) => s.interrupt
      case None => ct.pure(false)
    }
  }

  def remove(pRef: ProcessRef): F[Option[Process[F]]] = {
    ct.delay(Option(processes.remove(pRef)).map(_.process))
  }

}

object Context {

  def apply[F[_] : Concurrent : ContextShift](config: Parapet.ParConfig,
                                              eventLog: EventLog[F]): F[Context[F]] = {
    implicitly[Concurrent[F]].delay(new Context[F](config, eventLog))
  }


  case class BlockingOp[F[_] : Concurrent](fiber: Fiber[F, Unit], deferred: Deferred[F, Unit])

  class AsyncOps[F[_] : Concurrent](process: Process[F]) {


    private val signals = new AtomicReference[List[BlockingOp[F]]](List.empty)
    private val ct = implicitly[Concurrent[F]]

    def add(f: Fiber[F, Unit], d: Deferred[F, Unit]): F[Unit] = {
      ct.delay(signals.updateAndGet(l => l :+ BlockingOp(f, d)))
    }

    def waitForCompletion: F[Unit] = {
      signals.get().map(o => o.deferred.get).sequence_
    }

    def clear: F[Unit] = {
      ct.delay(signals.set(List.empty))
    }

    def size: F[Int] = ct.pure(signals.get.size)

    def completeAll: F[Unit] = for {
      _ <- signals.get.map(_.fiber.cancel).sequence_
      _ <- signals.get.map(_.deferred.complete(())).sequence_
    } yield ()
  }

  class ProcessState[F[_] : Concurrent](
                                         queue: TaskQueue[F],
                                         lock: Lock[F],
                                         val process: Process[F],
                                         _interruption: Deferred[F, Unit]) {

    import ProcessState._

    private val ct = implicitly[Concurrent[F]]

    private[this] val _interrupted: AtomicBoolean = new AtomicBoolean(false)
    private[this] val _stopped: AtomicBoolean = new AtomicBoolean(false)
    private[this] val _suspended: AtomicBoolean = new AtomicBoolean(false)
    val blocking: AsyncOps[F] = new AsyncOps(process)
    private[this] val pLock = new ProcessLock[F](process.ref)

    def tryPut(t: Task[F]): F[Boolean] = {
      queue.tryEnqueue(t)
    }

    def tryTakeTask: F[Option[Task[F]]] = queue.tryDequeue

    def interrupt: F[Boolean] = {
      ct.delay(_interrupted.compareAndSet(false, true)).flatMap {
        case true => _interruption.complete(()).map(_ => true)
        case false => ct.pure(false)
      }
    }

    def stop(): F[Boolean] = ct.delay(_stopped.compareAndSet(false, true))

    def interruption: Deferred[F, Unit] = _interruption

    def interrupted: F[Boolean] = ct.pure(_interrupted.get())

    def stopped: F[Boolean] = ct.pure(_stopped.get())

    def isStopped: Boolean = _stopped.get

    def acquired: F[Boolean] = pLock.acquired

    def acquire: F[Boolean] = pLock.acquire

    def release: F[Boolean] = pLock.release

    def releaseNow: F[Unit] = pLock.releaseNow


    /**
      * returns `true` if this process performing some blocking operations, other `false`
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
    def apply[F[_] : Concurrent : ContextShift](process: Process[F],
                                                config: Parapet.ParConfig): F[ProcessState[F]] = {
      val processBufferSize = if (process.bufferSize != -1) process.bufferSize else config.processBufferSize
      for {
        queue <-
          if (processBufferSize == -1) Queue.unbounded[F, Task[F]]()
          else Queue.bounded[F, Task[F]](processBufferSize, ChannelType.SPSC)
        lock <- Lock[F]
        terminated <- Deferred[F, Unit]
      } yield new ProcessState[F](queue, lock, process, terminated)
    }

    class ProcessLock[F[_] : Concurrent](ref: ProcessRef) {
      private val lock = new AtomicBoolean()
      private val nLock = new AtomicBoolean()
      private[this] val ct = implicitly[Concurrent[F]]

      def acquired: F[Boolean] = ct.delay {
        nLock.set(true)
        lock.get()
      }

      def acquire: F[Boolean] = ct.delay {
        lock.compareAndSet(false, true)
      }

      def releaseNow: F[Unit] = ct.delay {
        throw new UnsupportedOperationException()

      }

      // release and reset
      def release: F[Boolean] =
        ct.delay {
          lock.compareAndSet(true, false)
          !nLock.compareAndSet(true, false)
        }
    }

  }

}
