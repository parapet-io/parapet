package io.parapet

import cats.effect.{Concurrent, ContextShift, Timer}
import io.parapet.core.{Context, DslInterpreter, Parallel}
import io.parapet.zioinstances.all._
import scalaz.zio.internal.{Platform, PlatformLive}
import scalaz.zio.interop.catz._
import scalaz.zio.interop.catz.implicits._
import scalaz.zio.{DefaultRuntime, Fiber, Task}

import scala.concurrent.ExecutionContext

trait ZioApp extends ParApp[Task] with DefaultRuntime {

  lazy val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  implicit lazy val runtime: DefaultRuntime = new DefaultRuntime {
    override val Platform: Platform =
      PlatformLive.fromExecutionContext(ec)
  }

  override lazy val contextShift: ContextShift[Task] = ContextShift[Task]

  override lazy val ct: Concurrent[Task] = Concurrent[Task]

  override lazy val parallel: Parallel[Task] = Parallel[Task]

  override lazy val timer: Timer[Task] = Timer[Task]

  override def flowInterpreter(context: Context[Task]): DslInterpreter.Interpreter[Task] =
    DslInterpreter[Task](context)

  override def unsafeRun(task: Task[Unit]): Unit = {
    runtime.unsafeRunSync(task.fork.flatMap { fiber =>
      installHook(fiber).map(_ => fiber)
    }.flatMap(_.join))
  }

  private def installHook(fiber: Fiber[Throwable, Unit]): Task[Unit] =
    Task {
      sys.addShutdownHook {
        // Should block the thread until all finalizers are executed
        runtime.unsafeRunSync(fiber.interrupt)
      }
    }
}
