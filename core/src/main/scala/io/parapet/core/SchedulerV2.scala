package io.parapet.core

import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.{Concurrent, ContextShift, Timer}
import io.parapet.core.Scheduler._
import io.parapet.core.SchedulerV2.Worker
import io.parapet.core.SchedulerV2.State
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import com.typesafe.scalalogging.Logger
import io.parapet.core.DslInterpreter._
import io.parapet.core.Event.Envelope
import org.slf4j.LoggerFactory

class SchedulerV2[F[_] : Concurrent : Timer : Parallel : ContextShift](
                                                                        config: SchedulerConfig,
                                                                        taskQueue: TaskQueue[F],
                                                                        processRefQueue: Queue[F, ProcessRef],
                                                                        processesMap: Map[ProcessRef, State[F]],
                                                                        interpreter: Interpreter[F]) extends Scheduler[F] {


  // todo use custom thread pool
  private val ctxShift = implicitly[ContextShift[F]]
  private val logger = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))

  override def run: F[Unit] = {
    val workers = createWorkers

    def step: F[Unit] = {
      taskQueue.dequeue >>= {
        case t@Deliver(Envelope(_, _, pRef)) =>
          processesMap(pRef).putTask(t) >> processRefQueue.enqueue(pRef) >> step
        case _: Terminate[F] => workers.map(_.stop).sequence.void
      }
    }

    implicitly[Parallel[F]].par(workers.map(w => ctxShift.shift >> w.run) :+ step)
  }

  override def submit(task: Task[F]): F[Unit] = {
    taskQueue.enqueue(task)
  }


  def createWorkers: List[Worker[F]] = {
    (1 to config.numberOfWorkers).map { i => {
      new Worker[F](s"worker-$i", processRefQueue, processesMap, interpreter)
    }
    }.toList
  }

}


object SchedulerV2 {


  def apply[F[_] : Concurrent : Parallel : Timer : ContextShift](
                                                                  config: SchedulerConfig,
                                                                  processes: Array[Process[F]],
                                                                  taskQueue: TaskQueue[F],
                                                                  interpreter: Interpreter[F]): F[Scheduler[F]] =
    for {
      processRefQueue <- Queue.unbounded[F, ProcessRef]
      processesMap <- processes.map(p => State(p).map(s => p.ref -> s)).toList.sequence.map(_.toMap)
    } yield
      new SchedulerV2(config, taskQueue, processRefQueue, processesMap, interpreter)

  class State[F[_] : Concurrent](
                                  queue: TaskQueue[F],
                                  lock: Lock[F],
                                  val process: Process[F],
                                  executing: AtomicBoolean = new AtomicBoolean()) {

    private val ct = implicitly[Concurrent[F]]

    def putTask(t: Task[F]): F[Unit] = {
      lock.withPermit(queue.enqueue(t))
    }

    def tryTakeTask: F[Option[Task[F]]] = queue.tryDequeue

    def acquire: F[Boolean] = ct.delay(executing.compareAndSet(false, true))

    def release: F[Boolean] = {
      if (!executing.get())
        ct.raiseError(new RuntimeException("process cannot be released because it wasn't acquired"))
      else {
        // lock required to avoid situation when worker 1 got suspended during process release,
        // scheduler puts a new task to the process's queue and process ref to processRefQueue
        // worker 2 dequeues process ref and fails to acquire it b/c it's still in executing state
        // thus new task will be lost
        // process must be released before scheduler will add it to processRefQueue
        lock.withPermit {
          queue.isEmpty >>= {
            case true =>
              ct.delay(executing.compareAndSet(true, false)) >>= {
                case false => ct.raiseError(new RuntimeException("concurrent release"))
                case _     => ct.pure(true)
              }
            case false => ct.pure(false) // new task available, don't release yet
          }
        }
      }
    }

  }

  object State {
    def apply[F[_] : Concurrent](process: Process[F]): F[State[F]] =
      for {
        queue <- Queue.unbounded[F, Task[F]]
        lock <- Lock[F]
      } yield new State[F](queue, lock, process)
  }


  class Worker[F[_] : Concurrent](name: String,
                                  processRefQueue: Queue[F, ProcessRef],
                                  processesMap: Map[ProcessRef, State[F]],
                                  interpreter: Interpreter[F]) {

    private val logger = Logger(LoggerFactory.getLogger(s"parapet-$name"))
    private val ct = implicitly[Concurrent[F]]

    def run: F[Unit] = {
      def step: F[Unit] = {
        processRefQueue.dequeue >>= { pRef =>
          val ps = processesMap(pRef)
          ps.acquire >>= {
            case true  => ct.delay(logger.debug(s"name acquired process $pRef")) >> run(ps) >> step
            case false => step
          }
        }
      }

      step
    }

    private def run(ps: State[F]): F[Unit] = {
      def step: F[Unit] = {
        ps.tryTakeTask >>= {
          case Some(t: Deliver[F]) => executeTask(ps.process, t) >> step
          case Some(_)             => ct.raiseError(new RuntimeException("unsupported task"))
          case None                => ps.release >>= {
              case true  => ct.unit  // done with this process
              case false => step    //  process still has some tasks
          }
        }
      }

      step
    }

    def executeTask(process: Process[F], t: Deliver[F]): F[Unit] = {
      val sender = t.envelope.sender
      val receiver = t.envelope.receiver
      val program = process.handle.apply(t.envelope.event)
      interpret_(program, interpreter, FlowState(senderRef = sender, selfRef = receiver))
    }

    def stop: F[Unit] = ct.unit
  }

}