package io.parapet

import cats.effect.{Concurrent, ContextShift, Fiber, Timer}
import io.parapet.core.{Context, DslInterpreter, Parallel}
import io.parapet.monixinstances.all._
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.ExecutionContext

trait MonixApp extends ParApp[Task] {
  lazy val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  implicit lazy val scheduler: Scheduler = Scheduler(ec)

  implicit lazy val contextShift: ContextShift[Task] = Task.contextShift(scheduler)
  override lazy val ct: Concurrent[Task] = Concurrent[Task]
  override lazy val parallel: Parallel[Task] = Parallel[Task]
  implicit lazy val timer: Timer[Task] = Task.timer(scheduler)

  override def flowInterpreter(context: Context[Task]): DslInterpreter.Interpreter[Task] =
    DslInterpreter[Task](context)

  override def unsafeRun(task: Task[Unit]): Unit = {
    task.start.flatMap { fiber =>
      installHook(fiber).map(_ => fiber)
    }.flatMap(_.join).runSyncUnsafe()
  }

  private def installHook(fiber: Fiber[Task, Unit]): Task[Unit] =
    Task {
      sys.addShutdownHook {
        // Should block the thread until all finalizers are executed
        fiber.cancel.runSyncUnsafe()
      }
    }
}
