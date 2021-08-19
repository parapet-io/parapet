package io.parapet.benchmark

import cats.effect.IO
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events._
import io.parapet.core.api.Event
import io.parapet.core.{Process, ProcessRef}
import io.parapet.{CatsApp, core}

import java.util.concurrent.TimeUnit

object Benchmark extends CatsApp {

  import dsl._

  val Total = 1000000

  private val counterRef = ProcessRef("counter")

  class Counter extends Process[IO] {
    var startTime = 0L
    var endTime = 0L
    private var count = 0L

    override val ref: ProcessRef = counterRef

    override def handle: Receive = {
      case Inc =>
        eval {
          count = count + 1
        }
      case Ready =>
        eval {
          startTime = System.nanoTime()
        }
      case Stop =>
        eval {
          endTime = System.nanoTime()
          val timeInMillis = TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS)
          println(s"{count: $count}, time: $timeInMillis millis")
        }
    }
  }

  object Ready extends Event

  object Inc extends Event

  class Sender(total: Int, counter: ProcessRef) extends Process[IO] {

    def send(i: Int): DslF[IO, Unit] = flow {
      if (i < total) {
        dsl.send(Inc, counter) ++ send(i + 1)
      } else {
        dsl.send(Stop, counter)
      }
    }

    override def handle: Receive = { case Start =>
      dsl.send(Ready, counter) ++ send(0)
    }
  }

  override def processes(args: Array[String]): IO[Seq[core.Process[IO]]] =
    IO {
      val total = args.headOption.map(Integer.parseInt).getOrElse(Total)
      val counter = new Counter()
      val sender = new Sender(total, counter.ref)
      List(counter, sender)
    }
}
