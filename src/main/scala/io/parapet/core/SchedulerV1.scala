package io.parapet.core

import java.util.concurrent.atomic.AtomicBoolean

import cats.data.OptionT
import cats.effect.concurrent.{Deferred, MVar, Ref}
import cats.effect.{Concurrent, ContextShift, Timer}
import cats.implicits._
import com.typesafe.scalalogging.Logger
import io.parapet.core.Event._
import io.parapet.core.Logging._
import io.parapet.core.Parapet.{Stop => _, _}
import io.parapet.core.ProcessRef.{DeadLetterRef, SystemRef}
import io.parapet.core.SchedulerV1._
import io.parapet.core.Scheduler._
import io.parapet.core.exceptions._
import io.parapet.syntax.effect._
import org.slf4j.LoggerFactory


import scala.collection.mutable
import scala.collection.mutable.{Map => MutMap}
import scala.concurrent.duration._
import scala.util.Random

class SchedulerV1 [F[_] : Concurrent : Timer : Parallel : ContextShift] (
                                                                          config: SchedulerConfig,
                                                                          processes: Array[Process[F]],
                                                                          queue: TaskQueue[F],
                                                                          assignedWorkersVar: MVar[F, Map[ProcessRef, Worker[F]]],
                                                                          idleWorkersRef: Ref[F, mutable.LinkedHashSet[String]],
                                                                          interpreter: Interpreter[F])  extends Scheduler[F] {


  private val logger = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))

  private val ce = implicitly[Concurrent[F]]
  private val parallel = implicitly[Parallel[F]]
  private val ctxShift = implicitly[ContextShift[F]]
  private val timer = implicitly[Timer[F]]

  def submit(task: Task[F]): F[Unit] = queue.enqueue(task)


  def getWorkerByName(allWorkers: List[Worker[F]], name: String): Worker[F] = {
    allWorkers.find(_.name == name).get
  }

  def consumer(allWorkers: List[Worker[F]]): F[Unit] = {
    queue.dequeue.flatMap {
      case Terminate() =>
        ce.delay(logger.debug("Scheduler - terminate")) >> terminateWorkers(allWorkers)
      case task@Deliver(Envelope(_, _, receiver)) => submit(receiver, task) >> consumer(allWorkers)
    }}

  private def submit(processRef: ProcessRef, task: Task[F]): F[Unit] = {
    val res = for {
      assignedWorkers <- assignedWorkersVar.take
      res <- assignedWorkers(processRef).submit(task, config.taskSubmissionTimeout)
      _ <- assignedWorkersVar.put(assignedWorkers)
    } yield res

    res >>= {
      case true => ce.unit
      case false => submit(processRef, task)
    }
  }


  override def run: F[Unit] = {
    val window = Math.ceil(processes.length.toDouble / config.numberOfWorkers.toDouble).toInt
    val groupedProcesses: List[(Array[Process[F]], Int)] =
      grow(processes.sliding(window, window).toList, Array.empty[Process[F]], config.numberOfWorkers).zipWithIndex

    val workersF: F[List[Worker[F]]] = groupedProcesses.map {
      case (processGroup, i) => Worker(i, config, assignedWorkersVar, idleWorkersRef, processGroup, interpreter)
    }.sequence

    ce.bracket(workersF) { workers =>
      val assignedWorkers: Map[ProcessRef, Worker[F]] = workers.flatMap(worker => worker.processes.keys.map(_ -> worker)).toMap
      assignedWorkersVar.put(assignedWorkers) >>
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

object SchedulerV1 {

  def apply[F[_] : Concurrent : Timer : Parallel : ContextShift](config: SchedulerConfig,
                                                                 processes: Array[Process[F]],
                                                                 queue: TaskQueue[F],
                                                                 interpreter: Interpreter[F]): F[Scheduler[F]] = {
    for {
      assignedWorkersVar <- MVar.empty[F, Map[ProcessRef, Worker[F]]]
      idleWorkersRef <- Ref.of[F, mutable.LinkedHashSet[String]](mutable.LinkedHashSet.empty)
    } yield new SchedulerV1[F](config,
      processes,
      queue,
      assignedWorkersVar,
      idleWorkersRef,
      interpreter)
  }



  class Worker[F[_] : Concurrent : Timer : Parallel](
                                                      val name: String,
                                                      val queue: TaskQueue[F],
                                                      var processes: MutMap[ProcessRef, Process[F]],
                                                      val assignedWorkersVar: MVar[F, Map[ProcessRef, Worker[F]]],
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
    private var stealingCounter = 0
    private val maxStealingAttempts = 3

    def submit(task: Task[F], duration: FiniteDuration): F[Boolean] = {
      queue.tryEnqueue(task, duration)
    }

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
          taskOpt <- ce.start(queue.tryDequeue(config.workerTaskDequeueTimeout)).flatMap(f => f.join).flatMap {
            case Some(t) => ce.pure(Option(t))
            case None => //ce.pure(Option.empty[Task[F]])
              if (stealingCounter == maxStealingAttempts) {
                ce.delay(stealingCounter = 0) >> queue.dequeue.map(t => Option(t))
              } else {
                ce.delay(stealingCounter = stealingCounter + 1) >>
                  ce.delay(logger.debug(s"$name is out of tasks, try to steal")) >> stealTasks
              }

          }
          _ <- taskOpt.fold(ce.pure(taskOpt))(task => currentTaskProcessRef.set(getReceiverRef(task)).map(_ => taskOpt))
          _ <- queueReadLock.release
        } yield taskOpt

        taskOpt.flatMap {
          case Some(task) =>
            task match {
              case t: Deliver[F] =>
                ce.delay(logger.debug(s"$name started processing event=[${t.envelope.event}] for  process ${t.envelope.receiver}")) >>
                  processTask(t) >>
                  ce.delay(logger.debug(s"$name finished processing event=[${t.envelope.event}] for process ${t.envelope.receiver}")) >>
                  currentTaskProcessRef.set(None) >> step
              case _: Terminate[F] => stop >> completionSignal.complete(())
            }
          case None => step
        }
      }

      step
    }


    def stealTasks: F[Option[Task[F]]] = {
      for {
        assignedWorkers <- assignedWorkersVar.take
        res <- OptionT(queue.tryDequeue).map(t => (assignedWorkers, List(t)))
          .orElse {
            OptionT(chooseWorker(assignedWorkers.values.toVector)).flatMap { victim =>
              OptionT(victim.steal(100.millis)).flatMapF {
                case (process, tasks) =>
                  ce.delay(logger.debug(s"$name stole ${tasks.size} tasks[$tasks] for process[${process.ref}] from ${victim.name}")) >>
                    addProcess(process).map(_ => Option((assignedWorkers + (process.ref -> self), tasks)))
              }
            }
          }.getOrElse((assignedWorkers, List.empty))
        taskOp <- res._2 match {
          case x :: xs => queue.enqueueAll(xs).map(_ => Option(x))
          case _ => ce.pure(Option.empty[Task[F]])
        }
        _ <- assignedWorkersVar.put(res._1)
      } yield taskOp
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

    def chooseWorker(workers: Vector[Worker[F]]): F[Option[Worker[F]]] = {
      for {
        candidates <- workers.filter(_.name != name).map(w => w.queue.size.map(s => (s, w))).toList.sequence
      } yield {
        if (candidates.isEmpty) None
        else Some(candidates.maxBy(_._1)._2)
      }
    }

    // todo review
    def steal(lockAcquireTimeout: FiniteDuration): F[Option[(Process[F], List[Task[F]])]] = {
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
                      case (left, right) => queue.enqueueAll(left) >> ce.pure((victim, right.toList))
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
                                                    assignedWorkersVar: MVar[F, Map[ProcessRef, Worker[F]]],
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
        assignedWorkersVar = assignedWorkersVar,
        qReadLock,
        idleWorkersRef = idleWorkersRef,
        config,
        interpreter,
        currentTaskProcessRef,
        completionSignal)
  }

}