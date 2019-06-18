package io.parapet

import cats.effect.{Concurrent, ContextShift, Timer}
import cats.implicits._
import cats.~>
import io.parapet.core.Dsl.FlowOps._
import io.parapet.core.Dsl.{Dsl, DslF, FlowOps}
import io.parapet.core.DslInterpreter._
import io.parapet.core.Event.Start
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.ProcessRef.SystemRef
import io.parapet.core.Scheduler.{SchedulerConfig, Task, TaskQueue}
import io.parapet.core.processes.{DeadLetterProcess, SystemProcess}
import io.parapet.core.{Parallel, Process, Queue, Scheduler}
import io.parapet.syntax.flow._

import scala.concurrent.duration._
import scala.language.{higherKinds, implicitConversions, reflectiveCalls}

abstract class ParApp[F[_]] {

  type Effect[A] = io.parapet.core.Dsl.Effect[F, A]
  type FlowOp[A] = io.parapet.core.Dsl.FlowOp[F, A]
  type Flow[A] = io.parapet.core.DslInterpreter.Flow[F, A]
  type Program = DslF[F, Unit]

  val config: ParConfig = ParApp.defaultConfig

  implicit val parallel: Parallel[F]
  implicit val timer: Timer[F]
  implicit val ct: Concurrent[F]
  implicit val contextShift: ContextShift[F]
  val processes: Array[Process[F]]

  // system processes
  def deadLetter: DeadLetterProcess[F] = DeadLetterProcess.logging

  private[parapet] lazy val systemProcess: Process[F] = new SystemProcess[F]

  def flowInterpreter(taskQueue: TaskQueue[F]): FlowOp ~> Flow

  def effectInterpreter: Effect ~> Flow

  val program: Program = implicitly[FlowOps[F, Dsl[F, ?]]].empty

  def unsafeRun(f: F[Unit]): Unit

  def stop: F[Unit]

  private[parapet] final def initProcesses(implicit F: FlowOps[F, Dsl[F, ?]]): Program =
    processes.map(p => F.send(Start, p.ref)).foldLeft(F.empty)(_ ++ _)

  def run: F[Unit] = {
    if (processes.isEmpty) {
      ct.raiseError(new RuntimeException("Initialization error:  at least one process must be provided"))
    } else {
      val systemProcesses = Array(systemProcess, deadLetter)
      for {
        taskQueue <- Queue.bounded[F, Task[F]](config.schedulerConfig.queueSize)
        //context <- ct.pure(AppContext(taskQueue))
        interpreter <- ct.pure(flowInterpreter(taskQueue) or effectInterpreter)
        scheduler <- Scheduler.apply2[F](config.schedulerConfig, systemProcesses ++ processes, taskQueue, interpreter)
        _ <- parallel.par(
          Seq(interpret_(initProcesses ++ program, interpreter, FlowState(SystemRef, SystemRef)),
            scheduler.run))
        _ <- stop
      } yield ()
    }
  }

  def main(args: Array[String]): Unit = {
    unsafeRun(run)
  }
}


object ParApp {
  val defaultConfig: ParConfig = ParConfig(
    schedulerConfig = SchedulerConfig(
      queueSize = 1000,
      numberOfWorkers = Runtime.getRuntime.availableProcessors(),
      workerQueueSize = 100,
      taskSubmissionTimeout = 5.seconds,
      workerTaskDequeueTimeout = 5.minutes,
      maxRedeliveryRetries = 5,
      redeliveryInitialDelay = 0.seconds))
}