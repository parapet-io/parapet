package io.parapet.core

import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger

import cats.effect.{ContextShift, IO, Timer}
import cats.effect.IO._
import cats.implicits._
import io.parapet.core.Parapet.ParapetPrefix
import scala.concurrent.duration._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object ShutdownSpec {

  val executorService: ExecutorService =
    Executors.newFixedThreadPool(
      Runtime.getRuntime.availableProcessors(), new ThreadFactory {
        val threadNumber = new AtomicInteger(1)
        override def newThread(r: Runnable): Thread =
          new Thread(r, ParapetPrefix + "thread-" + threadNumber.getAndIncrement())
      })
  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(executorService)
  implicit val timer: Timer[IO] = IO.timer(executionContext)
  implicit val contextShift: ContextShift[IO] = IO.contextShift(executionContext)
  def main(args: Array[String]): Unit = {
    IO.race(List(loop, loop).parSequence_,
      IO.sleep(3.seconds) >>
      IO(println("shutdownNow")) >>
      IO(println(executorService.shutdownNow())) >> IO(println("shutdownNow-done"))

    ).unsafeRunSync()
  }

  def loop: IO[Unit] = IO.unit.flatMap(_ => contextShift.shift >> IO.sleep(5.millis) >> loop)
}
