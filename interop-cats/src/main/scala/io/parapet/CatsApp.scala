package io.parapet

import cats.effect._
import cats.~>
import com.typesafe.scalalogging.Logger
import io.parapet.core.{Context, DslInterpreter, Parallel}
import org.slf4j.LoggerFactory
import io.parapet.catsnstances.parallel._

import scala.concurrent.ExecutionContext

abstract class CatsApp extends ParApp[IO] {

  lazy val logger = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))

  lazy val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  implicit lazy val contextShift: ContextShift[IO] = IO.contextShift(ec)
  override lazy val ct: Concurrent[IO] = Concurrent[IO]
  override lazy val parallel: Parallel[IO] = Parallel[IO]
  implicit lazy val timer: Timer[IO] = IO.timer(ec)

  override def flowInterpreter(context: Context[IO]): FlowOp ~> Flow = {
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
