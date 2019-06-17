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
import io.parapet.core.Scheduler._
import io.parapet.core.exceptions._
import io.parapet.syntax.effect._
import org.slf4j.LoggerFactory


import scala.collection.mutable
import scala.collection.mutable.{Map => MutMap}
import scala.concurrent.duration._
import scala.util.Random

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