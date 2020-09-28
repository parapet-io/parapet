package io.parapet

import cats.effect._
import io.parapet.catsnstances.all._
import io.parapet.core.{Context, DslInterpreter, Parallel}

import scala.concurrent.ExecutionContext

trait CatsApp extends ParApp[IO] {

  lazy val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  implicit lazy val contextShift: ContextShift[IO] = IO.contextShift(ec)
  override lazy val ct: Concurrent[IO] = Concurrent[IO]
  override lazy val parallel: Parallel[IO] = Parallel[IO]
  implicit lazy val timer: Timer[IO] = IO.timer(ec)

  override def flowInterpreter(context: Context[IO]): DslInterpreter.Interpreter[IO] = {
    DslInterpreter[IO](context)
  }

  override def unsafeRun(io: IO[Unit]): Unit = {
    io.start.flatMap { fiber =>
      installHook(fiber).map(_ => fiber)
    }.flatMap(_.join).unsafeRunSync()
  }

  private def installHook(fiber: Fiber[IO, Unit]): IO[Unit] =
    IO {
      sys.addShutdownHook {
        // Should block the thread until all finalizers are executed
        fiber.cancel.unsafeRunSync()
      }
    }
}
