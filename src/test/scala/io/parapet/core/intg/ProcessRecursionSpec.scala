package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Parapet._
import io.parapet.core.catsInstances.effect._
import io.parapet.core.catsInstances.flow._
import io.parapet.core.intg.ProcessRecursionSpec._
import org.scalatest.{FlatSpec, Matchers, OptionValues}

class ProcessRecursionSpec extends FlatSpec with Matchers with OptionValues {

  "Stack safe" should "deliver Stop message to each process" in {
    var deliverCount = 0
    val count = 100000
    val p = new Process[IO] {
      override val handle: Receive = {
        case Counter(i) =>
          if (i == 0) terminate
          else eval {
            deliverCount = deliverCount + 1
          } ++ Counter(i - 1) ~> selfRef
      }
    }

    val program = Counter(count) ~> p

    run(program, p)

    deliverCount shouldBe count

  }

  def run(pProgram: FlowF[IO, Unit], pProcesses: Process[IO]*): Unit = {
    val app = new CatsApp {
      override val program: ProcessFlow = pProgram

      override val processes: Array[Process[IO]] = pProcesses.toArray
    }
    app.run.unsafeRunSync()
  }
}

object ProcessRecursionSpec {

  case class Counter(value: Int) extends Event

}