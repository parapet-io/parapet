package io.parapet.core

import cats.effect.{Concurrent, ContextShift, Timer}
import io.parapet.core.DslInterpreter.Interpreter
import io.parapet.core.Event._
//import io.parapet.core.Parapet.{Stop => _}
import io.parapet.core.Scheduler._

import scala.concurrent.duration._

trait Scheduler[F[_]] {

  def run: F[Unit]
  def submit(task: Task[F]): F[Unit]
}

object Scheduler {

  sealed trait Task[F[_]]

  case class Deliver[F[_]](envelope: Envelope) extends Task[F]

  case class Terminate[F[_]]() extends Task[F]

  type TaskQueue[F[_]] = Queue[F, Task[F]]

  def apply1[F[_] : Concurrent : Timer : Parallel : ContextShift](config: SchedulerConfig,
                                                                 processes: Array[Process[F]],
                                                                 queue: TaskQueue[F],
                                                                 interpreter: Interpreter[F]): F[Scheduler[F]] = {
    SchedulerV1.apply(config, processes, queue, interpreter)
  }


  def apply2[F[_] : Concurrent : Timer : Parallel : ContextShift](config: SchedulerConfig,
                                                                  processes: Array[Process[F]],
                                                                  queue: TaskQueue[F],
                                                                  interpreter: Interpreter[F]): F[Scheduler[F]] = {
    SchedulerV2.apply(config, processes, queue, interpreter)
  }

  case class SchedulerConfig(queueSize: Int,
                             numberOfWorkers: Int,
                             workerQueueSize: Int,
                             taskSubmissionTimeout: FiniteDuration,
                             workerTaskDequeueTimeout: FiniteDuration,
                             maxRedeliveryRetries: Int,
                             redeliveryInitialDelay: FiniteDuration)


}