package io.parapet.core

import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import cats.effect.{Concurrent, ContextShift, Timer}
import io.parapet.core.Scheduler._
import io.parapet.core.Parapet.ParapetPrefix
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.effect.syntax.bracket._
import cats.syntax.applicativeError._
import cats.syntax.traverse._
import com.typesafe.scalalogging.Logger
import io.parapet.core.Dsl.{Dsl, FlowOps}
import io.parapet.core.DslInterpreter._
import io.parapet.core.Event.{DeadLetter, Envelope, Failure, Start, Stop}
import io.parapet.core.ProcessRef._
import org.slf4j.LoggerFactory
import io.parapet.core.exceptions._

import scala.concurrent.ExecutionContext

trait Scheduler[F[_]] {
  def run: F[Unit]

  def submit(task: Task[F]): F[Unit]
}

object Scheduler {

  sealed trait Task[F[_]]

  case class Deliver[F[_]](envelope: Envelope) extends Task[F]

  case class Terminate[F[_]]() extends Task[F]

  type TaskQueue[F[_]] = Queue[F, Task[F]]


  def apply[F[_] : Concurrent : Timer : Parallel : ContextShift](config: SchedulerConfig,
                                                                 processes: Array[Process[F]],
                                                                 queue: TaskQueue[F],
                                                                 eventDeliveryHooks: EventDeliveryHooks[F],
                                                                 interpreter: Interpreter[F]): F[Scheduler[F]] = {
    SchedulerImpl(config, processes, queue, eventDeliveryHooks, interpreter)
  }

  case class SchedulerConfig(queueSize: Int,
                             numberOfWorkers: Int,
                             processQueueSize: Int)

  import SchedulerImpl._

  class SchedulerImpl[F[_] : Concurrent : Timer : Parallel : ContextShift](
                                                                            config: SchedulerConfig,
                                                                            taskQueue: TaskQueue[F],
                                                                            processRefQueue: Queue[F, ProcessRef],
                                                                            processesStates: Map[ProcessRef, State[F]],
                                                                            eventDeliveryHooks: EventDeliveryHooks[F],
                                                                            interpreter: Interpreter[F]) extends Scheduler[F] {


    private val ct = Concurrent[F]
    private val pa = implicitly[Parallel[F]]
    private val logger = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))


    private val es: ExecutorService =
      Executors.newFixedThreadPool(
        config.numberOfWorkers, new ThreadFactory {
          val threadNumber = new AtomicInteger(1)

          override def newThread(r: Runnable): Thread =
            new Thread(r, s"$ParapetPrefix-scheduler-thread-pool-${threadNumber.getAndIncrement()}")
        })

    private val ec: ExecutionContext = ExecutionContext.fromExecutor(es)
    private val ctxShift = implicitly[ContextShift[F]]

    override def run: F[Unit] = {
      val workers = createWorkers

      def step: F[Unit] = {
        ctxShift.evalOn(ec)(taskQueue.dequeue) >>= {
          case t@Deliver(e@Envelope(_, _, pRef)) =>
            processesStates.get(pRef)
              .fold(sendToDeadLetter(DeadLetter(e,
                UnknownProcessException(s"there is no such process with id=$pRef registered in the system")),
                interpreter) >> step) { pState =>
                pState.tryPut(t) >>= {
                  case true => processRefQueue.enqueue(pRef) >> step
                  case false =>
                    sendToDeadLetter(
                      DeadLetter(e,
                        EventDeliveryException(s"process ${pState.process} event queue is full. send event to deadletter")),
                      interpreter) >> step
                }
              }
          case t => ct.raiseError(new RuntimeException(s"unsupported task: $t"))
        }
      }

      pa.par(workers.map(w => ctxShift.shift >> w.run) :+ (ctxShift.shift >> step))
        .guaranteeCase(_ => {
          deliverStopEvent(processesStates.values.map(_.process).toList) >>
            ct.delay(es.shutdownNow()).void.guaranteeCase(_ => ct.delay(logger.info("scheduler has been shut down")))
        })
    }

    override def submit(task: Task[F]): F[Unit] = {
      taskQueue.enqueue(task)
    }

    private def createWorkers: List[Worker[F]] = {
      (1 to config.numberOfWorkers).map { i => {
        new Worker[F](s"worker-$i", processRefQueue, processesStates, eventDeliveryHooks, interpreter, ec)
      }
      }.toList
    }

    private def deliverStopEvent(processes: Seq[Process[F]]): F[Unit] = {
      pa.par(processes.map { process =>
        if (process.execute.isDefinedAt(Stop)) {
          ctxShift.evalOn(ec)(interpret_(
            process.execute.apply(Stop),
            interpreter, FlowState(senderRef = SystemRef, selfRef = process.selfRef)))
            .handleErrorWith(err => ct.delay(logger.error(s"An error occurred while stopping process $process", err)))
        } else {
          ct.unit
        }

      })
    }

  }


  object SchedulerImpl {

    def apply[F[_] : Concurrent : Parallel : Timer : ContextShift](
                                                                    config: SchedulerConfig,
                                                                    processes: Array[Process[F]],
                                                                    taskQueue: TaskQueue[F],
                                                                    eventDeliveryHooks: EventDeliveryHooks[F],
                                                                    interpreter: Interpreter[F]): F[Scheduler[F]] =
      for {
        processRefQueue <- Queue.bounded[F, ProcessRef](config.queueSize)
        processesMap <- processes.map(p => State(p, config.processQueueSize).map(s => p.selfRef -> s)).toList.sequence.map(_.toMap)
      } yield
        new SchedulerImpl(config, taskQueue, processRefQueue, processesMap, eventDeliveryHooks, interpreter)

    class State[F[_] : Concurrent](
                                    queue: TaskQueue[F],
                                    lock: Lock[F],
                                    val process: Process[F],
                                    executing: AtomicBoolean = new AtomicBoolean()) {

      private val ct = implicitly[Concurrent[F]]

      def tryPut(t: Task[F]): F[Boolean] = {
        lock.withPermit(queue.tryEnqueue(t))
      }

      def tryTakeTask: F[Option[Task[F]]] = queue.tryDequeue

      def acquire: F[Boolean] = ct.delay(executing.compareAndSet(false, true))

      def release: F[Boolean] = {
        if (!executing.get())
          ct.raiseError(new RuntimeException("process cannot be released because it wasn't acquired"))
        else {
          // lock required to avoid the situation when worker 1 got suspended during process release,
          // scheduler puts a new task to the process's queue and process ref to processRefQueue
          // worker 2 dequeues process ref and fails to acquire it b/c it's still in executing state
          // thus new task will be lost
          // process must be released before scheduler will add it to processRefQueue
          lock.withPermit {
            queue.isEmpty >>= {
              case true =>
                ct.delay(executing.compareAndSet(true, false)) >>= {
                  case false => ct.raiseError(new RuntimeException("concurrent release"))
                  case _ => ct.pure(true)
                }
              case false => ct.pure(false) // new task available, don't release yet
            }
          }
        }
      }

    }

    object State {
      def apply[F[_] : Concurrent](process: Process[F], queueSize: Int): F[State[F]] =
        for {
          queue <- Queue.bounded[F, Task[F]](queueSize)
          lock <- Lock[F]
        } yield new State[F](queue, lock, process)
    }


    class Worker[F[_] : Concurrent : ContextShift](name: String,
                                                   processRefQueue: Queue[F, ProcessRef],
                                                   processesMap: Map[ProcessRef, State[F]],
                                                   eventDeliveryHooks: EventDeliveryHooks[F],
                                                   interpreter: Interpreter[F],
                                                   ec: ExecutionContext) {

      private val logger = Logger(LoggerFactory.getLogger(s"parapet-$name"))
      private val ct = implicitly[Concurrent[F]]
      private val ctx = implicitly[ContextShift[F]]

      def run: F[Unit] = {
        def step: F[Unit] = {
          ctx.evalOn(ec)(processRefQueue.dequeue) >>= { pRef =>
            val ps = processesMap(pRef)
            ps.acquire >>= {
              case true => ct.delay(logger.debug(s"name acquired process ${ps.process} for processing")) >>
                ctx.evalOn(ec)(run(ps)) >> step
              case false => step
            }
          }
        }

        step
      }

      private def run(ps: State[F]): F[Unit] = {
        def step: F[Unit] = {
          ps.tryTakeTask >>= {
            case Some(t: Deliver[F]) => deliver(ps.process, t.envelope) >> step
            case Some(t) => ct.raiseError(new RuntimeException(s"unsupported task: $t"))
            case None => ps.release >>= {
              case true => ct.delay(logger.debug(s"$name has been released process ${ps.process}"))
              case false => step // process still has some tasks, continue
            }
          }
        }

        step
      }

      private def deliver(process: Process[F], envelope: Envelope): F[Unit] = {
        val event = envelope.event
        val sender = envelope.sender
        val receiver = envelope.receiver

        val completeAwaitOnDeliver = eventDeliveryHooks.removeFirstMatch(process.selfRef, event) match {
          case Some(hook) =>
            ct.delay(logger.debug(s"process $process complete delivery hook for event: $event")) >>
              hook.d.complete(())
          case None => ct.unit
        }

        val deliver = if (process.execute.isDefinedAt(event)) {
          val program = process.execute.apply(event)
          interpret_(program, interpreter, FlowState(senderRef = sender, selfRef = receiver))
            .handleErrorWith(err => handleError(process, envelope, err))
        } else {
          val errorMsg = s"process $process handler is not defined for event: $event"
          val whenUndefined = event match {
            case f: Failure =>
              // no error handling, send to dead letter
              sendToDeadLetter(DeadLetter(f), interpreter)
            case Start => ct.unit // ignore lifecycle events
            case _ => sendToDeadLetter(DeadLetter(envelope, EventMatchException(errorMsg)), interpreter)
          }
          ct.delay(logger.warn(errorMsg)) >> whenUndefined
        }

        completeAwaitOnDeliver >> ct.delay(logger.debug(s"$name deliver $event to process $process")) >> deliver
      }

      private def handleError(process: Process[F], envelope: Envelope, cause: Throwable): F[Unit] = {
        val event = envelope.event

        event match {
          case f: Failure =>
            ct.delay(logger.error(s"process $process has failed to handle Failure event. send to deadletter", cause)) >>
              sendToDeadLetter(DeadLetter(f), interpreter)
          case _ =>
            val errMsg = s"process $process has failed to handle event: $event"
            ct.delay(logger.error(errMsg, cause)) >>
              send(SystemRef, Failure(envelope, EventHandlingException(errMsg, cause)), envelope.sender, interpreter)

        }
      }

    }

    private def sendToDeadLetter[F[_] : Concurrent](dl: DeadLetter, interpreter: Interpreter[F])(implicit flowDsl: FlowOps[F, Dsl[F, ?]]): F[Unit] = {
      send(SystemRef, dl, DeadLetterRef, interpreter)
    }

    private def send[F[_] : Concurrent](sender: ProcessRef,
                                        event: Event,
                                        receiver: ProcessRef,
                                        interpreter: Interpreter[F])(implicit flowDsl: FlowOps[F, Dsl[F, ?]]): F[Unit] = {
      interpret_(flowDsl.send(event, receiver), interpreter,
        FlowState(senderRef = sender, selfRef = receiver))
    }

  }


}