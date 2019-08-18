package io.parapet.core

import cats.effect.syntax.bracket._
import cats.effect.{Concurrent, ContextShift}
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.typesafe.scalalogging.Logger
import io.parapet.core.Context.ProcessState
import io.parapet.core.Dsl.{Dsl, FlowOps}
import io.parapet.core.DslInterpreter._
import io.parapet.core.Event._
import io.parapet.core.ProcessRef._
import io.parapet.core.Queue.ChannelType
import io.parapet.core.Scheduler._
import io.parapet.core.exceptions._
import org.slf4j.LoggerFactory

trait Scheduler[F[_]] {
  def run: F[Unit]

  def submit(task: Task[F]): F[Unit]
}

object Scheduler {

  sealed trait Task[F[_]]

  case class Deliver[F[_]](envelope: Envelope) extends Task[F]

  type TaskQueue[F[_]] = Queue[F, Task[F]]

  def apply[F[_] : Concurrent : Parallel : ContextShift](config: SchedulerConfig,
                                                         context: Context[F],
                                                         interpreter: Interpreter[F]): F[Scheduler[F]] = {
    SchedulerImpl(config, context, interpreter)
  }

  case class SchedulerConfig(queueSize: Int,
                             numberOfWorkers: Int,
                             processQueueSize: Int) {
    require(queueSize > 0 || queueSize == -1)
    require(numberOfWorkers > 0)
    require(processQueueSize > 0 || processQueueSize == -1)
  }

  import SchedulerImpl._

  class SchedulerImpl[F[_] : Concurrent : Parallel : ContextShift](
                                                                    config: SchedulerConfig,
                                                                    context: Context[F],
                                                                    processRefQueue: Queue[F, ProcessRef],
                                                                    interpreter: Interpreter[F]) extends Scheduler[F] {


    private val ct = Concurrent[F]
    private val pa = implicitly[Parallel[F]]
    private val logger = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))

    override def run: F[Unit] = {
      val workers = createWorkers

      def step: F[Unit] = {
        context.taskQueue.dequeue >>= {
          case t@Deliver(e@Envelope(sender, event, pRef)) =>
            context.getProcessState(pRef)
              .fold(send(
                SystemRef,
                Failure(e, UnknownProcessException(s"there is no such process with id=$pRef registered in the system")),
                sender,
                interpreter) >> step) { ps =>
                (event match {
                  case Kill =>
                    context.interrupt(pRef) >>
                      submit(Deliver(Envelope(sender, Stop, pRef)), ps)
                  // interruption isn't instant operation since it's just a signal (e.g. Deferred)
                  // i.e. interrupt may be completed although
                  // the actual process is still performing some computations and will be interrupted latter
                  // we need to submit Stop event here instead of direct call
                  // to avoid race condition between interruption and process stop
                  case _ => submit(t, ps)
                }) >> step
              }
          case t => ct.raiseError(new RuntimeException(s"unsupported task: $t"))
        }
      }

      pa.par(workers.map(w => w.run) :+ step)
        .guarantee(
          stopProcess(ProcessRef.SystemRef,
            context, ProcessRef.SystemRef, interpreter,
            (pRef, err) => ct.delay(logger.error(s"An error occurred while stopping process $pRef", err)))) >>
        ct.delay(logger.info("scheduler has been shut down"))
    }

    private def submit(task: Deliver[F], ps: ProcessState[F]): F[Unit] = {
      ps.tryPut(task) >>= {
        case true => processRefQueue.enqueue(ps.process.ref)
        case false =>
          send(ProcessRef.SystemRef, Failure(task.envelope,
            EventDeliveryException(s"System failed to deliver an event to process ${ps.process}",
              EventQueueIsFullException(s"process ${ps.process} event queue is full"))),
            task.envelope.sender, interpreter)
      }
    }

    override def submit(task: Task[F]): F[Unit] = {
      context.taskQueue.enqueue(task)
    }

    private def createWorkers: List[Worker[F]] = {
      (1 to config.numberOfWorkers).map { i => {
        new Worker[F](s"worker-$i", context, processRefQueue, interpreter)
      }
      }.toList
    }


  }


  object SchedulerImpl {

    def apply[F[_] : Concurrent : Parallel : ContextShift](
                                                            config: SchedulerConfig,
                                                            context: Context[F],
                                                            interpreter: Interpreter[F]): F[Scheduler[F]] =
      for {
        processRefQueue <-
        if (config.queueSize == -1) Queue.unbounded[F, ProcessRef](ChannelType.SPMC)
        else Queue.bounded[F, ProcessRef](config.queueSize, ChannelType.SPMC)
      } yield
        new SchedulerImpl(
          config,
          context,
          processRefQueue,
          interpreter)

    class Worker[F[_] : Concurrent : Parallel : ContextShift](name: String,
                                                              context: Context[F],
                                                              processRefQueue: Queue[F, ProcessRef],
                                                              interpreter: Interpreter[F]) {

      private val logger = Logger(LoggerFactory.getLogger(s"parapet-$name"))
      private val ct = implicitly[Concurrent[F]]

      def run: F[Unit] = {
        def step: F[Unit] = {
          processRefQueue.dequeue >>= { pRef =>
            context.getProcessState(pRef) match {
              case Some(ps) =>
                ps.acquire >>= {
                  case true => run(ps) >> step
                  case false => step
                }
              case None => step // process was terminated and removed from the system,
              // eventually scheduler will stop delivering new events for this process
            }

          }
        }

        step
      }

      private def run(ps: ProcessState[F]): F[Unit] = {
        def step: F[Unit] = {
          ps.tryTakeTask >>= {
            case Some(t: Deliver[F]) => deliver(ps, t.envelope) >> step
            case Some(t) => ct.raiseError(new RuntimeException(s"unsupported task: $t"))
            case None => ps.release >>= {
              case None => ct.delay(logger.debug(s"$name has been released process ${ps.process}"))
              case Some(task: Deliver[F]) => deliver(ps, task.envelope) >>
                step // process still has some tasks, continue
            }
          }
        }

        step
      }

      private def deliver(processState: ProcessState[F], envelope: Envelope): F[Unit] = {
        val process = processState.process
        val event = envelope.event
        val sender = envelope.sender
        val receiver = envelope.receiver


        event match {
          case Stop if processState.stop() =>
            stopProcess(sender, context, process.ref, interpreter,
              (_, err) => handleError(process, envelope, err)) >> context.remove(process.ref).void
          case _ if processState.stopped =>
            sendToDeadLetter(
              DeadLetter(envelope, new IllegalStateException(s"process: $process is stopped")), interpreter)
          case _ if processState.interrupted => { // process was interrupted but not stopped yet
            sendToDeadLetter(
              DeadLetter(envelope, new IllegalStateException(s"process: $process is terminated")), interpreter)
          }
          case _ =>
            if (process.canHandle(event)) {
              ct.race(
                (for {
                  flow <- ct.delay(process(event))
                  _ <- interpret_(flow, interpreter, FlowState[F](senderRef = sender, selfRef = receiver))
                } yield ()).handleErrorWith(err => handleError(process, envelope, err)),
                processState.interruption).flatMap {
                case Left(_) => ct.unit
                case Right(_) => ct.unit // process has been interrupted. Stop event shall be delivered by scheduler
              }
            } else {
              val errorMsg = s"process $process handler is not defined for event: $event"
              val whenUndefined = event match {
                case f: Failure =>
                  // no error handling, send to dead letter
                  sendToDeadLetter(DeadLetter(f), interpreter)
                case Start => ct.unit // ignore lifecycle events
                case _ =>
                  send(ProcessRef.SystemRef,
                    Failure(envelope, EventMatchException(errorMsg)), envelope.sender, interpreter)
                // sendToDeadLetter(DeadLetter(envelope, EventMatchException(errorMsg)), interpreter)
              }
              val logMsg = event match {
                case Start | Stop => ct.unit
                case _ => ct.delay(logger.warn(errorMsg))
              }
              logMsg >> whenUndefined
            }
        }
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
              sendErrorToSender(envelope, EventHandlingException(errMsg, cause))
        }
      }

      private def sendErrorToSender(envelope: Envelope, err: Throwable): F[Unit] = {
        send(SystemRef, Failure(envelope, err), envelope.sender, interpreter)
      }

    }

    private def sendToDeadLetter[F[_] : Concurrent](dl: DeadLetter, interpreter: Interpreter[F])
                                                   (implicit flowDsl: FlowOps[F, Dsl[F, ?]]): F[Unit] = {
      send(SystemRef, dl, DeadLetterRef, interpreter)
    }

    private def send[F[_] : Concurrent](sender: ProcessRef,
                                        event: Event,
                                        receiver: ProcessRef,
                                        interpreter: Interpreter[F])(implicit flowDsl: FlowOps[F, Dsl[F, ?]]): F[Unit] = {
      interpret_(flowDsl.send(event, receiver), interpreter,
        FlowState[F](senderRef = sender, selfRef = receiver))
    }

    private def deliverStopEvent[F[_] : Concurrent](sender: ProcessRef,
                                                    process: Process[F],
                                                    interpreter: Interpreter[F]): F[Unit] = {
      val ct = implicitly[Concurrent[F]]
      if (process.canHandle(Stop)) {
        interpret_(
          process(Stop),
          interpreter,
          FlowState[F](senderRef = sender, selfRef = process.ref))
      } else {
        ct.unit
      }
    }

    private def stopProcess[F[_] : Concurrent : Parallel](parent: ProcessRef,
                                                          context: Context[F],
                                                          ref: ProcessRef,
                                                          interpreter: Interpreter[F],
                                                          onError: (ProcessRef, Throwable) => F[Unit]): F[Unit] = {
      val ct = implicitly[Concurrent[F]]
      val pa = implicitly[Parallel[F]]

      ct.suspend {
        val stopChildProcesses =
          pa.par(context.child(ref).map(child => stopProcess(ref, context, child, interpreter, onError)))

        stopChildProcesses >>
          (context.getProcess(ref) match {
            case Some(p) => deliverStopEvent(parent, p, interpreter).handleErrorWith(err => onError(ref, err))
            case None => ct.unit
          })
      }
    }

  }


}