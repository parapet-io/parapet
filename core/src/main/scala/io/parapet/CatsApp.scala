package io.parapet

import cats.effect._
import cats.syntax.flatMap._
import cats.~>
import com.typesafe.scalalogging.Logger
import io.parapet.core.DslInterpreter.Dependencies
import io.parapet.core.Parallel
import io.parapet.instances.DslInterpreterInstances.dslInterpreterForCatsIO
import io.parapet.instances.parallel._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.global

abstract class CatsApp extends ParApp[IO] {

  val logger = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))

  implicit val contextShift: ContextShift[IO] = IO.contextShift(global)
  override val parallel: Parallel[IO] = Parallel[IO]
  implicit val timer: Timer[IO] = IO.timer(global)
  override val ct: Concurrent[IO] = implicitly[Concurrent[IO]]

  override def flowInterpreter(dependencies: Dependencies[IO]): FlowOp ~> Flow = {
    dslInterpreterForCatsIO.ioFlowInterpreter(dependencies)
  }

  override def effectInterpreter: Effect ~> Flow = {
    dslInterpreterForCatsIO.ioEffectInterpreter
  }

  override def unsafeRun(io: IO[Unit]): Unit = {
    io.start.flatMap { fiber =>
      installHook(fiber).map(_ => fiber)
    }.flatMap(_.join).unsafeRunSync()
  }

  override def stop: IO[Unit] = IO(logger.info("shutdown")) >> IO(unsafeStop)

  def unsafeStop: Unit = {
    //executorService.shutdownNow()
  }

  private def installHook(fiber: Fiber[IO, Unit]): IO[Unit] =
    IO {
      sys.addShutdownHook {
        // Should block the thread until all finalizers are executed
        fiber.cancel.unsafeRunSync()
      }
    }
}
