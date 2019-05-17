package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Parapet._
import io.parapet.core.catsInstances.effect._
import io.parapet.core.catsInstances.flow._
import org.scalatest.{FlatSpec, Matchers, OptionValues}

//todo non deterministic
class ProcessTerminationSpec extends FlatSpec with Matchers with OptionValues {

  "Terminate" should "deliver Stop message to each process" in {
    var stopEventsCount = 0
    val p1 = Process[IO] {
      case Stop => eval {
        stopEventsCount = stopEventsCount + 1
      }
    }
    val p2 = Process[IO] {
      case Stop => eval {
        stopEventsCount = stopEventsCount + 1
      }
    }

    run(terminate, p1, p2)

    stopEventsCount shouldBe 2
  }

  def run(pProgram: FlowF[IO, Unit], pProcesses: Process[IO]*): Unit = {
    val app = new CatsApp {
      override val program: ProcessFlow = pProgram

      override val processes: Array[Process[IO]] = pProcesses.toArray
    }
    app.run.unsafeRunSync()
  }

}
