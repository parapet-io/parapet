package io.parapet.core

import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, ContextShift, Timer}
import cats.implicits._
import com.typesafe.scalalogging.Logger
import io.parapet.core.Event._
import io.parapet.core.Logging._
import io.parapet.core.Parapet.{ParapetPrefix, Interpreter, FlowState, interpret_, Flow, FlowOpOrEffect}
import io.parapet.core.ProcessRef.{DeadLetterRef, SystemRef}
import io.parapet.core.Scheduler._
import io.parapet.core.exceptions._
import org.slf4j.LoggerFactory
import io.parapet.syntax.effect._

import scala.collection.immutable.{Queue => SQueue}
import scala.collection.mutable
import scala.collection.mutable.{Map => MutMap}
import scala.concurrent.duration._
import scala.util.Random

class Scheduler[F[_] : Concurrent : Timer : Parallel : ContextShift](
                                                                      config: SchedulerConfig,
                                                                      processes: Array[Process[F]],
                                                                      queue: TaskQueue[F],
                                                                      lock: Lock[F],
                                                                      assignedWorkersRef: Ref[F, Map[ProcessRef, Worker[F]]],
                                                                      idleWorkersRef: Ref[F, mutable.LinkedHashSet[String]],
                                                                      interpreter: Interpreter[F]) {


  private val logger = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))

  private val ce = implicitly[Concurrent[F]]
  private val parallel = implicitly[Parallel[F]]
  private val ctxShift = implicitly[ContextShift[F]]
  private val timer = implicitly[Timer[F]]

  def submit(task: Task[F]): F[Unit] = queue.enqueue(task)

  // randomly selects a victim from { w | w ∈ workers and w != excludeWorker }
  def selectVictim(excludeWorker: Worker[F], workers: Vector[Worker[F]]): Worker[F] = {
    val filtered = workers.filter(w => w.name != excludeWorker.name)
    filtered(Random.nextInt(filtered.size))
  }


  // todo review
  def submitTask(task: Deliver[F],
                 assignedWorkers: Map[ProcessRef, Worker[F]],
                 allWorkers: List[Worker[F]]): F[Map[ProcessRef, Worker[F]]] = {
    //val receiver = task.envelope.receiver
    val worker = assignedWorkers(task.envelope.receiver)
    ce.race(worker.queue.enqueue(task), timer.sleep(config.taskSubmissionTimeout)).flatMap {
      case Left(_) =>
        ce.delay(logger.debug(
          s"successfully submitted task[pRef=${task.envelope.receiver}, event=${task.envelope.event}] to worker ${worker.name}")) >>
          // remove `worker` from idle workers
          idleWorkersRef.update(idleWorkers => idleWorkers.filter(_ != worker.name)) >>
          ce.pure(assignedWorkers)
      case Right(_) => submitTask(task, assignedWorkers, allWorkers) // no idle workers, resubmit task using current worker

        val victimOpt = idleWorkersRef.modify { idleWorkers =>
          idleWorkers.toList match {
            case x :: xs => (mutable.LinkedHashSet(xs:_*), Some(x))
            case _ => (idleWorkers, None)
          }
        }

        victimOpt.flatMap {
          case Some(victimName) =>
            val victim = getWorkerByName(allWorkers, victimName)
            ce.delay(logger.debug(s"try to steal tasks from ${worker.name}")) >>
              worker.steal(100.millis).flatMap {
                case Some((vp, removedTasks)) =>
                  ce.delay {
                    val tasks = removedTasks :+ task
                    val reassignedWorkers = assignedWorkers + (vp.ref -> victim)
                    (tasks, reassignedWorkers)
                  } flatMap {
                    case (tasks, reassignedWorkers) =>
                      ce.delay(logger.debug(s"Transfer ${tasks.size} tasks ${tasks.map(_.envelope.event)} from ${worker.name} to worker ${victim.name}")) >>
                        // todo problem could be here
                        // assignedWorkersRef.set(reassignedWorkers) >> todo revisit
                        victim.addProcess(vp) >>
                        submitTasks(tasks.toList, reassignedWorkers, allWorkers)
                    //                  tasks.map(t => submitTask(t, reassignedWorkers, allWorkers)).toList.sequence_
                    //                    .map(_ => reassignedWorkers)
                  }
                case None => submitTask(task, assignedWorkers, allWorkers) // no tasks to steal
              }
          case None => submitTask(task, assignedWorkers, allWorkers) // no idle workers, resubmit task using current worker
        }
    }

  }

  def getWorkerByName(allWorkers: List[Worker[F]], name: String): Worker[F] = {
    allWorkers.find(_.name == name).get
  }

  def submitTasks(tasks: List[Deliver[F]], assignedWorkers: Map[ProcessRef, Worker[F]],
                  allWorkers: List[Worker[F]]): F[Map[ProcessRef, Worker[F]]] = {
    tasks match {
      case x :: xs => submitTask(x, assignedWorkers, allWorkers) >>=
        (reassignedWorkers => submitTasks(xs, reassignedWorkers, allWorkers))
      case _ => ce.pure(assignedWorkers)
    }
  }

  def consumer(allWorkers: List[Worker[F]]): F[Unit] = {
    queue.dequeue.flatMap {
      case Terminate() =>
        ce.delay(logger.debug("Scheduler - terminate")) >> terminateWorkers(allWorkers)
      //  todo review
      case task@Deliver(Envelope(_, _, receiver)) =>
        lock.acquire >>
          assignedWorkersRef.get >>= { assignedWorkers =>
          assignedWorkers.get(receiver)
            .fold(ce.delay(logger.warn(s"unknown process: $receiver. Ignore event")) >> ce.pure(assignedWorkers)) {
              _ => submitTask(task, assignedWorkers, allWorkers)
            } >>= (wm => assignedWorkersRef.set(wm) >> lock.release >> consumer(allWorkers))
        }

    }
  }

  def run: F[Unit] = {
    val window = Math.ceil(processes.length.toDouble / config.numberOfWorkers.toDouble).toInt
    val groupedProcesses: List[(Array[Process[F]], Int)] =
      grow(processes.sliding(window, window).toList, Array.empty[Process[F]], config.numberOfWorkers).zipWithIndex

    val workersF: F[List[Worker[F]]] = groupedProcesses.map {
      case (processGroup, i) => Worker(i, config, lock, assignedWorkersRef,idleWorkersRef, processGroup, interpreter)
    }.sequence

    ce.bracket(workersF) { workers =>
      val assignedWorkers: Map[ProcessRef, Worker[F]] = workers.flatMap(worker => worker.processes.keys.map(_ -> worker)).toMap
      assignedWorkersRef.set(assignedWorkers) >>
      ce.delay(logger.debug(s"initially assigned workers: ${assignedWorkers.mapValues(_.name)}")) >>
        parallel.par(workers.map(worker => ctxShift.shift >> worker.run) :+ consumer(workers))
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

  def apply[F[_] : Concurrent : Timer : Parallel : ContextShift](config: SchedulerConfig,
                                                                 processes: Array[Process[F]],
                                                                 queue: TaskQueue[F],
                                                                 interpreter: Interpreter[F]): F[Scheduler[F]] = {
    for {
      lock <- Lock[F]
      assignedWorkersRef <- Ref.of[F, Map[ProcessRef, Worker[F]]](Map.empty)
      idleWorkersRef <- Ref.of[F, mutable.LinkedHashSet[String]](mutable.LinkedHashSet.empty)
    } yield new Scheduler[F](config,
      processes,
      queue,
      lock,
      assignedWorkersRef,
      idleWorkersRef,
      interpreter)
  }

  case class SchedulerConfig(queueSize: Int,
                             numberOfWorkers: Int,
                             workerQueueSize: Int,
                             taskSubmissionTimeout: FiniteDuration,
                             workerTaskDequeueTimeout: FiniteDuration,
                             maxRedeliveryRetries: Int,
                             redeliveryInitialDelay: FiniteDuration)

  class Worker[F[_] : Concurrent : Timer : Parallel](
                                                      val name: String,
                                                      val queue: TaskQueue[F],
                                                      var processes: MutMap[ProcessRef, Process[F]],
                                                      val schedulerLock: Lock[F],
                                                      val assignedWorkersRef: Ref[F, Map[ProcessRef, Worker[F]]],
                                                      val queueReadLock: Lock[F],
                                                      val idleWorkersRef: Ref[F, mutable.LinkedHashSet[String]],
                                                      config: SchedulerConfig,
                                                      interpreter: Interpreter[F],
                                                      currentTaskProcessRef: Ref[F, Option[ProcessRef]],
                                                      completionSignal: Deferred[F, Unit]) {  self=>
    private val logger = Logger(LoggerFactory.getLogger(s"parapet-$name"))
    private val ce = implicitly[Concurrent[F]]
    private val timer = implicitly[Timer[F]]
    private val parallel = implicitly[Parallel[F]]
    private val flowOps = implicitly[Flow[F, FlowOpOrEffect[F, ?]]]
    private[this] val stopped = new AtomicBoolean()
    private val idle = new AtomicBoolean()

    def deliver(envelope: Envelope): F[Unit] = {
      val event = envelope.event
      val sender = envelope.sender
      val receiver = envelope.receiver
      currentTaskProcessRef.get.flatMap { currProcessRef =>
        ce.fromOption(processes.get(receiver),
          new UnknownProcessException(
            s"$name unknown process: $receiver, event=${envelope.event}, currProcess = $currProcessRef"))
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

    // todo review
    def run: F[Unit] = {
      def step: F[Unit] = {
        val taskOpt = for {
          _ <- queueReadLock.acquire
          taskOpt <- ce.race(queue.dequeue, timer.sleep(config.workerTaskDequeueTimeout)).flatMap {
            case Left(t) => ce.pure(Option(t))
            case Right(_) => ce.delay(logger.debug(s"$name is out of tasks, try to steal")) >> stealTasks
          }
          _ <- taskOpt.fold(ce.pure(taskOpt))(task => currentTaskProcessRef.set(getReceiverRef(task)).map(_ => taskOpt))
          _ <- queueReadLock.release
        } yield taskOpt

        taskOpt.flatMap {
          case Some(task) =>
            removeSelfToIdleList >>
              (
                task match {
                  case t: Deliver[F] =>
                    ce.delay(logger.debug(s"$name started processing event=[${t.envelope.event}] for  process ${t.envelope.receiver}")) >>
                      processTask(t) >>
                      ce.delay(logger.debug(s"$name finished processing event=[${t.envelope.event}] for process ${t.envelope.receiver}"))>>
                      currentTaskProcessRef.set(None) >> step
                  case _: Terminate[F] => stop >> completionSignal.complete(())
                }
              )
          case None => step
        }
      }

      step
    }

    // todo review
    def stealTasks: F[Option[Task[F]]] = {
      addSelfToIdleList >>
        schedulerLock.tryAcquire(100.millis).flatMap {
          case true =>
            // check the worker queue to avoid double stealing
            queue.tryDequeue.flatMap {
              case Some(task) => ce.pure(Option(task.asInstanceOf[Deliver[F]]))
              case None =>
                chooseWorker.flatMap {
                  case Some(victim) =>
                    ce.delay(logger.debug(s"$name chose ${victim.name} as victim")) >>
                      victim.steal(100.millis).flatMap {
                        case Some((process, tasks)) =>
                          ce.pure(processes.put(process.ref, process)) >>
                            ce.delay(logger.debug(s"$name stole ${tasks.size} tasks for process[ref=${process.ref}] from ${victim.name}")) >>
                            assignedWorkersRef.update(m => m + (process.ref -> self)) >>
                            removeSelfToIdleList >> // to prevent scheduler from using this worker for work balancing
                            (tasks.dequeueOption match {
                              case Some((head, tail)) => queue.enqueueAll(tail) >> ce.pure(Option(head))
                              case None => ce.pure(Option.empty[Deliver[F]]) // stolen process has no events yet
                            })
                        case None =>
                          ce.delay(logger.debug(s"$name has failed to steal a process from victim: ${victim.name}"))
                          ce.pure(Option.empty[Deliver[F]])
                      }
                  case None => ce.delay(
                    logger.debug(s"$name has failed to find a victim for balancing, apparently b/c number of workers ==  1")) >>
                    ce.pure(Option.empty[Deliver[F]])
                }
            } >>= (t => schedulerLock.release.map(_ => t))
          case false => ce.pure(Option.empty[Task[F]])
        }
    }

    def addSelfToIdleList: F[Unit] = {
      if (idle.compareAndSet(false, true))
        idleWorkersRef.update(idleWorkers => mutable.LinkedHashSet(idleWorkers.toSeq :+ self.name: _*))
      else ce.unit
    }

    def removeSelfToIdleList: F[Unit] = {
      if (idle.compareAndSet(true, false))
        idleWorkersRef.update(idleWorkers => idleWorkers.filter(_ != self.name))
      else ce.unit
    }

    def chooseWorker: F[Option[Worker[F]]] = {
      val rnd = Random
      for {
        workers <- assignedWorkersRef.get
      } yield {
        val candidates = workers.values.filter(_.name != name).toVector
        if (candidates.isEmpty) None
        else Some(candidates.maxBy(_.queue.size))
      }
    }

    // waits indefinitely while the given process is used by some task
//    def waitForProcess(pRef: ProcessRef): F[Unit] = {
//      //ce.delay(logger.debug(s"await process: $pRef")) >>
//      currentTaskProcessRef.get flatMap {
//        case Some(processInUse) if processInUse == pRef => waitForProcess(pRef)
//        case e => ce.delay(logger.debug(s"process $pRef is finished: $e"))
//      }
//    }

    // todo review
    def steal(lockAcquireTimeout: FiniteDuration): F[Option[(Process[F], SQueue[Deliver[F]])]] = {
      // try acquire to avoid deadlock
      queueReadLock.tryAcquire(lockAcquireTimeout).flatMap {
        case true =>
          chooseProcess.flatMap {
            case Some(victimRef) =>
              // it's safe to a remove process here b/c next the time when `processTask` is executed
              // this queue wont contain any events dedicated to the removed process
              ce.delay(logger.debug(s"$name removing process $victimRef")) >>
                ce.suspend(ce.fromOption(processes.remove(victimRef),
                  new RuntimeException(s"$name does not contain process with ref=$victimRef")))
                  .flatMap { victim =>
                    queue.partition {
                      case Deliver(envelop) => envelop.receiver == victimRef
                      case _ => false
                    }.flatMap {
                      case (left, right) => queue.enqueueAll(left) >> ce.pure((victim, SQueue(right.map(_.asInstanceOf[Deliver[F]]): _*)))
                    }
                  } >>= (tasks => queueReadLock.release.map(_ => Some(tasks)))
            case None => ce.delay(s"$name failed to choose a victim process") >>
              queueReadLock.release >> ce.pure(None)
          }

        case false => ce.delay(s"failed to acquire $name queue lock") >> ce.pure(None)
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


    def chooseProcess: F[Option[ProcessRef]] = {
      for {
        currProcess <- currentTaskProcessRef.get
      } yield currProcess match {
        case Some(ref) => randomProcess(ref)
        case None => randomProcess
      }
    }

    // randomly choose a process != exclude
    def randomProcess(exclude: ProcessRef): Option[ProcessRef] = {
      val rnd = Random
      val candidates = processes.keys.filter(_ != exclude).toVector
      if (candidates.isEmpty) None
      else Some(candidates(rnd.nextInt(candidates.size)))
    }

    // randomly choose a process
    def randomProcess: Option[ProcessRef] = randomProcess(processes.keys.toVector)

    def randomProcess(candidates: Vector[ProcessRef]): Option[ProcessRef] = {
      val rnd = Random
      if (candidates.isEmpty) None
      else Some(candidates(rnd.nextInt(candidates.size)))
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
                                                    schedulerLock: Lock[F],
                                                    assignedWorkersRef: Ref[F, Map[ProcessRef, Worker[F]]],
                                                    idleWorkersRef: Ref[F, mutable.LinkedHashSet[String]],
                                                    processes: Seq[Process[F]],
                                                    interpreter: Interpreter[F]): F[Worker[F]] =
      for {
        workerQueue <- Queue.bounded[F, Task[F]](config.workerQueueSize)
        qReadLock <- Lock[F]
        completionSignal <- Deferred[F, Unit]
        currentTaskProcessRef <- Ref.of[F, Option[ProcessRef]](None)
      } yield new Worker(
        name = s"$ParapetPrefix-worker-$id",
        queue = workerQueue,
        processes = MutMap(processes.map(p => p.ref -> p): _*),
        schedulerLock = schedulerLock,
        assignedWorkersRef = assignedWorkersRef,
        qReadLock,
        idleWorkersRef = idleWorkersRef,
        config,
        interpreter,
        currentTaskProcessRef,
        completionSignal)
  }

}
