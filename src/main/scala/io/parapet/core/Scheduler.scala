package io.parapet.core

import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, ContextShift, Timer}
import cats.implicits._
import com.typesafe.scalalogging.Logger
import io.parapet.core.Logging._
import io.parapet.core.Parapet.ProcessRef.{DeadLetterRef, SystemRef}
import io.parapet.core.Parapet._
import io.parapet.core.Scheduler._
import org.slf4j.LoggerFactory

import scala.collection.immutable.{Queue => SQueue}
import scala.collection.mutable.{Map => MutMap}
import scala.concurrent.duration._
import scala.util.Random

class Scheduler[F[_] : Concurrent : Timer : Parallel: ContextShift](
                                                                           queue: TaskQueue[F],
                                                                           config: SchedulerConfig,
                                                                           processes: Array[Process[F]],
                                                                           interpreter: Interpreter[F]) {





  private val logger = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))

  private val ce = implicitly[Concurrent[F]]
  private val parallel = implicitly[Parallel[F]]
  private val ctxShift = implicitly[ContextShift[F]]
  private val timer = implicitly[Timer[F]]

  private val processMap = processes.map(p => p.ref -> p).toMap

  def submit(task: Task[F]): F[Unit] = queue.enqueue(task)

  // randomly selects a victim from { w | w ∈ workers and w != excludeWorker }
  def selectVictim(excludeWorker: Worker[F], workers: Vector[Worker[F]]): Worker[F] = {
    val filtered = workers.filter(w => w.name != excludeWorker.name)
    filtered(Random.nextInt(filtered.size))
  }


  def submitTask(task: Deliver[F],
                 assignedWorkers: Map[ProcessRef, Worker[F]],
                 allWorkers: List[Worker[F]]): F[Map[ProcessRef, Worker[F]]] = {
    val receiver = task.envelope.receiver
    val worker = assignedWorkers(task.envelope.receiver)
    ce.race(worker.queue.enqueue(task), timer.sleep(config.taskSubmissionTimeout)).flatMap {
      case Left(_) =>
        ce.delay(logger.debug(
          s"successfully submitted task[pRef=${task.envelope.receiver}, event=${task.envelope.event()}] to worker ${worker.name}"))>>
          ce.pure(assignedWorkers)
      case Right(_) =>
        val victim = selectVictim(worker, allWorkers.toVector)
        worker.removeEvents(receiver, 100.millis).flatMap {
          case Some(removedTasks) =>
            ce.delay {
              val tasks = removedTasks :+ task
              val reassignedWorkers = assignedWorkers + (receiver -> victim)
              (tasks, reassignedWorkers)
            }  flatMap {
              case  (tasks, reassignedWorkers) =>
                ce.delay(logger.debug(s"Transfer ${tasks.size} tasks ${tasks.map(_.envelope.event())} from ${worker.name} to worker ${victim.name}")) >>
                  victim.addProcess(processMap(receiver)) >>
                  submitTasks(tasks.toList, reassignedWorkers, allWorkers)
//                  tasks.map(t => submitTask(t, reassignedWorkers, allWorkers)).toList.sequence_
//                    .map(_ => reassignedWorkers)
            }

          case None => submitTask(task, assignedWorkers, allWorkers) //  failed to acquire worker's queue lock, try again
        }
    }

  }
  def submitTasks(tasks: List[Deliver[F]], assignedWorkers: Map[ProcessRef, Worker[F]],
                  allWorkers: List[Worker[F]]): F[Map[ProcessRef, Worker[F]]] = {
    tasks match {
      case x :: xs => submitTask(x, assignedWorkers, allWorkers) >>=
        (reassignedWorkers => submitTasks(xs, reassignedWorkers, allWorkers))
      case _ => ce.pure(assignedWorkers)
    }
  }

  def consumer(assignedWorkers: Map[ProcessRef, Worker[F]], allWorkers: List[Worker[F]]): F[Unit] = {
    queue.dequeue.flatMap {
      case Terminate() =>
        ce.delay(logger.debug("Scheduler - terminate")) >> terminateWorkers(allWorkers)
      case task@Deliver(Envelope(_, _, receiver)) =>
        assignedWorkers.get(receiver)
          .fold(ce.delay(logger.warn(s"unknown process: $receiver. Ignore event")) >> ce.pure(assignedWorkers)) {
            _ => submitTask(task, assignedWorkers, allWorkers)
          } >>= (wm => consumer(wm, allWorkers))
    }
  }

  def run: F[Unit] = {
    val window = Math.ceil(processes.length.toDouble / config.numberOfWorkers.toDouble).toInt
    val groupedProcesses: List[(Array[Process[F]], Int)] =
      grow(processes.sliding(window, window).toList, Array.empty[Process[F]], config.numberOfWorkers).zipWithIndex

    val workersF: F[List[Worker[F]]] = groupedProcesses.map {
      case (processGroup, i) => Worker(i, config, processGroup, interpreter)
    }.sequence

    ce.bracket(workersF) { workers =>
      val processToWorker: Map[ProcessRef, Worker[F]] = workers.flatMap(worker => worker.processes.keys.map(_ -> worker)).toMap
      ce.delay(logger.debug(s"initially assigned workers: ${processToWorker.mapValues(_.name)}")) >>
      parallel.par(workers.map(worker => ctxShift.shift >> worker.run) :+ consumer(processToWorker, workers))
    } { workers => parallel.par(workers.map(_.stop)) }

  }

  def terminateWorkers(workers: List[Worker[F]]): F[Unit] = {
    parallel.par(workers.map(w => w.queue.enqueue(Terminate()))) >>
      workers.map(w => w.waitForCompletion).sequence_
  }

  def grow[A](xs: List[A], a: A, size: Int): List[A] = xs ++ List.fill(size - xs.size)(a)
}

object Scheduler {


  sealed trait Task[F[_]]
  case class Deliver[F[_]](envelope: Envelope) extends Task[F]
  case class Terminate[F[_]]() extends Task[F]

  type TaskQueue[F[_]] = Queue[F, Task[F]]

  def apply[F[_] : Concurrent : Timer: Parallel: ContextShift](queue: TaskQueue[F],
                                       config: SchedulerConfig,
                                       processes: Array[Process[F]], interpreter: Interpreter[F]): Scheduler[F] = {
    new Scheduler[F](queue, config, processes, interpreter)
  }

  case class SchedulerConfig(numberOfWorkers: Int,
                             queueCapacity: Int,
                             workerQueueCapacity: Int, //  todo rename
                             taskSubmissionTimeout: FiniteDuration,
                             maxRedeliveryRetries: Int,
                             redeliveryInitialDelay: FiniteDuration)

  class Worker[F[_] : Concurrent : Timer : Parallel](
                                                            val name: String,
                                                            val queue: TaskQueue[F],
                                                            var processes: MutMap[ProcessRef, Process[F]],
                                                            val queueReadLock: Lock[F],
                                                            config: SchedulerConfig,
                                                            interpreter: Interpreter[F],
                                                            currentTaskProcessRef: Ref[F, Option[ProcessRef]],
                                                            completionSignal: Deferred[F, Unit]) {
    private val logger = Logger(LoggerFactory.getLogger(s"parapet-$name"))
    private val ce = implicitly[Concurrent[F]]
    private val parallel = implicitly[Parallel[F]]
    private val flowOps = implicitly[Flow[F, FlowOpOrEffect[F, ?]]]
    private [this] val stopped = new AtomicBoolean()

    def deliver(envelope: Envelope): F[Unit] = {
      val event = envelope.event()
      val sender = envelope.sender
      val receiver = envelope.receiver
      currentTaskProcessRef.get.flatMap { currProcessRef =>
        ce.fromOption(processes.get(receiver),
          new UnknownProcessException(
            s"$name unknown process: $receiver, event=${envelope.event()}, currProcess = $currProcessRef"))
          .flatMap { process =>
            if (process.handle.isDefinedAt(event)) {
              val program = process.handle.apply(event)
              interpret_(program, interpreter, FlowState(senderRef = sender, selfRef = receiver))
                .retryWithBackoff(config.redeliveryInitialDelay, config.maxRedeliveryRetries)
                .handleErrorWith { deliveryError =>
                  val mdcFields: MDCFields = Map(
                    "processId" -> receiver,
                    "processName" -> process.name,
                    "senderId" -> sender,
                    "eventId" -> event.id,
                    "worker" -> name)
                  val logError = ce.delay {
                    logger.mdc(mdcFields) { args =>
                      logger.error(
                        s"process[id=${args("processId")}] failed to process event[id=${args("eventId")}] " +
                          s"received from process[id=${args("senderId")}]", deliveryError)
                    }
                  }

                  val recover = event match {
                    case _: Failure =>
                      val failure = Failure(receiver, event, new EventRecoveryException(cause = deliveryError))
                      ce.delay {
                        logger.mdc(mdcFields) { args =>
                          logger.error(s"process[id=${args("processId")}] failed to recover event[id=${args("eventId")}]", deliveryError)
                        }
                      } >> sendToDeadLetter(failure)
                    case _ =>
                      // send failure back to sender
                      val failure = Failure(receiver, event, new EventDeliveryException(cause = deliveryError))
                      interpret_(flowOps.send(failure, sender), interpreter,
                        FlowState(senderRef = SystemRef, selfRef = sender))
                  }

                  logError >> recover
                }
            } else {
              event match {
                case f: Failure =>
                  ce.delay(logger.warn(s"recovery logic isn't defined in process[id=$receiver]")) >>
                    sendToDeadLetter(f)
                case Stop | Start => ce.unit
                case _ => ce.unit // todo add config property: failOnUndefined
              }
            }
          }
      }


    }

    def sendToDeadLetter(failure: Failure): F[Unit] = {
      interpret_(
        flowOps.send(DeadLetter(failure), DeadLetterRef),
        interpreter, FlowState(senderRef = failure.pRef, selfRef = DeadLetterRef))
    }

    def processTask(task: Task[F]): F[Unit] = {
      task match {
        case Deliver(envelope) => ce.suspend(deliver(envelope))
        case unknown => ce.raiseError(new RuntimeException(s"$name - unsupported task: $unknown"))
      }
    }

    def getReceiverRef(task: Task[F]): Option[ProcessRef] = {
      task match {
        case Deliver(envelope) => Some(envelope.receiver)
        case _ => None
      }
    }

    def run: F[Unit] = {
      def step: F[Unit] = {
        val task = for {
          _ <- queueReadLock.acquire
          task <- queue.dequeue
          _ <- currentTaskProcessRef.set(getReceiverRef(task))
          _ <- queueReadLock.release
        } yield task

        task.flatMap {
          case t: Deliver[F] =>
            ce.delay(logger.debug(s"$name started processing event=[${t.envelope.event()}] for  process ${t.envelope.receiver}")) >>
            processTask(t) >> currentTaskProcessRef.set(None) >> step
          case _: Terminate[F] => stop >> completionSignal.complete(())
        }
      }
      step
    }

    // waits indefinitely while the given process is used by some task
    def waitForProcess(pRef: ProcessRef): F[Unit] = {
      //ce.delay(logger.debug(s"await process: $pRef")) >>
      currentTaskProcessRef.get flatMap {
        case Some(processInUse) if processInUse == pRef => waitForProcess(pRef)
        case e => ce.delay(logger.debug(s"process $pRef is finished: $e"))
      }
    }

    // removes all events dedicated to `pRef`
    def removeEvents(pRef: ProcessRef, lockAcquireTimeout: FiniteDuration): F[Option[SQueue[Deliver[F]]]] = {
      // try acquire to avoid deadlock
      queueReadLock.tryAcquire(lockAcquireTimeout).flatMap {
        case true =>
          waitForProcess(pRef) >>
          // it's safe to a remove process here b/c next the time when `processTask` is executed
          // this queue wont contain any events dedicated to the removed process
          ce.delay(logger.debug(s"$name removing process $pRef")) >>
          ce.suspend(ce.fromOption(processes.remove(pRef),
            new RuntimeException(s"$name does not contain process with ref=$pRef"))) // todo use unknown process exception
            .flatMap { _ =>
            queue.partition {
              case Deliver(envelop) => envelop.receiver == pRef
              case _ => false
            }.flatMap {
              case (left, right) => queue.enqueueAll(left) >> ce.pure(SQueue(right.map(_.asInstanceOf[Deliver[F]]): _*))
            }
          } >>= (tasks => queueReadLock.release.map(_ => Some(tasks)))
        case false => ce.pure(None)
      }
    }

    def deliverStopEvent: F[Unit] = {
      parallel.par(processes.values.map { process =>
        ce.delay(logger.debug(s"$name - deliver Stop to ${process.ref}")) >>
          (if (process.handle.isDefinedAt(Stop)) {
            interpret_(process.handle.apply(Stop), interpreter, FlowState(senderRef = SystemRef, selfRef = process.ref))
          } else {
            ce.unit
          })
      }.toSeq)
    }

    def stop: F[Unit] = {
      if (stopped.compareAndSet(false, true)) {
        ce.delay(logger.debug(s"$name - stop")) >> deliverStopEvent
      } else ce.unit
    }

    def waitForCompletion: F[Unit] = completionSignal.get >> ce.delay(logger.debug(s"$name completed"))

    def addProcess(p: Process[F]): F[Unit] = ce.delay(processes.put(p.ref, p))
  }

  object Worker {
    def apply[F[_] : Concurrent : Timer : Parallel](id: Int,
                                                          config: SchedulerConfig,
                                                          processes: Seq[Process[F]],
                                                          interpreter: Interpreter[F]): F[Worker[F]] =
      for {
        workerQueue <- Queue.bounded[F, Task[F]](config.workerQueueCapacity)
        qReadLock <- Lock[F]
        completionSignal <- Deferred[F, Unit]
        currentTaskProcessRef <- Ref.of[F, Option[ProcessRef]](None)
      } yield new Worker(
        s"$ParapetPrefix-worker-$id",
        workerQueue,
        MutMap(processes.map(p => p.ref -> p): _*),
        qReadLock,
        config,
        interpreter,
        currentTaskProcessRef,
        completionSignal)
  }

}
