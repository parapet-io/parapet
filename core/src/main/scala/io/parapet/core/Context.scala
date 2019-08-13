package io.parapet.core

import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, ContextShift}
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import io.parapet.core.Context._
import io.parapet.core.Event.{Envelope, Start}
import io.parapet.core.Queue.ChannelType
import io.parapet.core.Scheduler.{Deliver, Task, TaskQueue}
import io.parapet.core.exceptions.UnknownProcessException
import io.parapet.core.processes.SystemProcess

import scala.collection.JavaConverters._

class Context[F[_] : Concurrent : ContextShift](
                                                 config: Parapet.ParConfig,
                                                 val eventLog: EventLog[F],
                                                 val taskQueue: TaskQueue[F]) {


  private val ct = implicitly[Concurrent[F]]
  private val processQueueSize = config.schedulerConfig.processQueueSize

  private val processes = new java.util.concurrent.ConcurrentHashMap[ProcessRef, ProcessState[F]]()

  private val graph = new java.util.concurrent.ConcurrentHashMap[ProcessRef, Vector[ProcessRef]]

  def init: F[Unit] = {
    ct.delay(new SystemProcess[F]()).flatMap { sysProcess =>
      ProcessState(sysProcess, processQueueSize).flatMap { s =>
        ct.delay(processes.put(sysProcess.ref, s)) >> start(sysProcess.ref)
      }
    }
  }

  def register(parent: ProcessRef, process: Process[F]): F[ProcessRef] = {
    if (!processes.containsKey(parent)) {
      ct.raiseError(UnknownProcessException(
        s"process cannot be registered because parent process with id=$parent doesn't exist"))
    } else {
      ProcessState(process, processQueueSize).flatMap { s =>
        if (processes.putIfAbsent(process.ref, s) != null)
          ct.raiseError(new RuntimeException(s"duplicated process. ref = ${process.ref}"))
        else {
          graph.computeIfAbsent(parent, _ => Vector())
          graph.computeIfPresent(parent, (_, v) => v :+ process.ref)
          start(process.ref).map(_ => process.ref)
        }
      }
    }
  }

  def child(parent: ProcessRef): Vector[ProcessRef] = graph.getOrDefault(parent, Vector.empty)

  private def start(processRef: ProcessRef): F[Unit] = {
    taskQueue.enqueue(Deliver(Envelope(ProcessRef.SystemRef, Start, processRef)))
  }

  def registerAll(parent: ProcessRef, processes: List[Process[F]]): F[List[ProcessRef]] = {
    processes.map(p => register(parent, p)).sequence
  }

  def getProcesses: List[Process[F]] = processes.values().asScala.map(_.process).toList

  def getRunningProcesses: List[Process[F]] =
    processes.values().asScala.filter(!_.stopped).map(_.process).toList

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
    for {
      taskQueue <- Queue.bounded[F, Task[F]](config.schedulerConfig.queueSize, ChannelType.MPSC)
    } yield new Context[F](config, eventLog, taskQueue)
  }

  class ProcessState[F[_] : Concurrent](
                                         queue: TaskQueue[F],
                                         lock: Lock[F],
                                         val process: Process[F],
                                         _interruption: Deferred[F, Unit]) {

    private val ct = implicitly[Concurrent[F]]

    private val _interrupted: AtomicBoolean = new AtomicBoolean(false)
    private val _stopped: AtomicBoolean = new AtomicBoolean(false)
    private val executing: AtomicBoolean = new AtomicBoolean()

    def tryPut(t: Task[F]): F[Boolean] = {
      lock.withPermit(queue.tryEnqueue(t))
    }

    def tryTakeTask: F[Option[Task[F]]] = queue.tryDequeue

    def interrupt: F[Boolean] = {
      ct.delay(_interrupted.compareAndSet(false, true)).flatMap {
        case true => _interruption.complete(()).map(_ => true)
        case false => ct.pure(false)
      }
    }

    def stop(): Boolean = _stopped.compareAndSet(false, true)

    def interruption: F[Unit] = _interruption.get

    def interrupted: Boolean = _interrupted.get()

    def stopped: Boolean = _stopped.get()

    def acquire: F[Boolean] = ct.delay(executing.compareAndSet(false, true))

    def release: F[Option[Task[F]]] = {
      if (!executing.get())
        ct.raiseError(new RuntimeException("process cannot be released because it wasn't acquired"))
      else {
        // lock required to avoid the situation when worker 1 got suspended during process release,
        // scheduler puts a new task to the process's queue and process ref to processRefQueue
        // worker 2 dequeues process ref and fails to acquire it b/c it's still in executing state
        // thus new task will be lost
        // process must be released before scheduler will add it to processRefQueue
        lock.withPermit {
          queue.tryDequeue >>= {
            case None =>
              ct.delay(executing.compareAndSet(true, false)) >>= {
                case false => ct.raiseError(new RuntimeException("concurrent release"))
                case _ => ct.pure(Option.empty)
              }
            case Some(task) => ct.pure(Option(task)) // new task available, don't release yet
          }
        }
      }
    }

  }

  object ProcessState {
    def apply[F[_] : Concurrent : ContextShift](process: Process[F], queueSize: Int): F[ProcessState[F]] =
      for {
        queue <-
        if (queueSize == -1) Queue.unbounded[F, Task[F]]()
        else Queue.bounded[F, Task[F]](queueSize, ChannelType.SPSC)
        lock <- Lock[F]
        terminated <- Deferred[F, Unit]
      } yield new ProcessState[F](queue, lock, process, terminated)
  }

}
