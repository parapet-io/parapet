package io.parapet.core

import cats.effect.{ContextShift, ExitCode, Fiber, IO, IOApp, Resource, Timer}
import cats.implicits._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object ResourceReleaseDemo {
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)


  def main(args: Array[String]): Unit = {

    val program = List(loop("1").guarantee(IO(println("loop-1 has been finished"))), loop("2")).parSequence_

    val io = program.guarantee(IO(println("program has been finished")))
    io.start.flatMap { fiber =>
      installHook(fiber).map(_ => fiber)
    }.flatMap(_.join).unsafeRunSync()


    //   val cancellable = loop
    //      .unsafeRunCancelable( _ =>  println("done"))
    //    println("next")
    //    sys.addShutdownHook {
    //      cancellable.unsafeRunSync()
    //      println("done")
    //    }

  }

  def loop(id: String): IO[Unit] = {
    def step(i: Int): IO[Unit] = {
      IO.suspend {
        IO(println(s"$id:step-$i")) *> IO.sleep(1.seconds) *> step(i + 1)
      }
    }

    step(0)
  }

  private def installHook(fiber: Fiber[IO, Unit]): IO[Unit] =
    IO {
      sys.addShutdownHook {
        // Should block the thread until all finalizers are executed
        fiber.cancel.unsafeRunSync()
      }
    }


}
