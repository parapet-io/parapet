package io.parapet

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}

import cats.effect._
import cats.syntax.flatMap._
import cats.~>
import com.typesafe.scalalogging.Logger
import io.parapet.core.Parallel
import io.parapet.core.Parapet._
import io.parapet.core.Scheduler.TaskQueue
import io.parapet.instances.DslInterpreterInstances.dslInterpreterForCatsIO
import io.parapet.instances.parallel._
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

abstract class CatsApp extends ParApp[IO] {

  val logger = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))

  val executorService: ExecutorService =
    Executors.newFixedThreadPool(
      Runtime.getRuntime.availableProcessors(), new ThreadFactory {
        val threadNumber = new AtomicInteger(1)

        override def newThread(r: Runnable): Thread =
          new Thread(r, s"$ParapetPrefix-thread-${threadNumber.getAndIncrement()}")
      })

  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(executorService)
  implicit val contextShift: ContextShift[IO] = IO.contextShift(executionContext)
  override val parallel: Parallel[IO] = Parallel[IO]
  implicit val timer: Timer[IO] = IO.timer(executionContext)
  override val ct: Concurrent[IO] = implicitly[Concurrent[IO]]

  override def flowInterpreter(taskQueue: TaskQueue[IO]): FlowOp ~> Flow = {
    dslInterpreterForCatsIO.ioFlowInterpreter(taskQueue)
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
    executorService.shutdownNow()
  }

  private def installHook(fiber: Fiber[IO, Unit]): IO[Unit] =
    IO {
      sys.addShutdownHook {
        // Should block the thread until all finalizers are executed
        fiber.cancel.unsafeRunSync()
      }
    }
}
