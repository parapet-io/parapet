package io.parapet.core.intg

import java.util.concurrent.atomic.AtomicInteger

import cats.effect.IO
import io.parapet.core.Parapet._
import io.parapet.core.catsInstances.effect._
import io.parapet.core.catsInstances.flow._
import io.parapet.core.intg.EventDeliverySpec._
import org.scalatest.{FlatSpec, Matchers, OptionValues}

class EventDeliverySpec extends FlatSpec with Matchers with OptionValues {

  "Event sent to each process" should "be eventually delivered" in {
    val counter = new AtomicInteger()
    val numOfProcesses = 1000
    val processes = createProcesses(numOfProcesses, () => counter.incrementAndGet())
    val program = processes.foldLeft(ø)((acc, p) => acc ++ QualifiedEvent(p.ref) ~> p) ++ terminate
    run(program, processes: _*)

    counter.get() shouldBe numOfProcesses
  }

  "Unmatched event" should "be ignored" in {
    val p = Process {
      case QualifiedEvent(_) => ø
    }
    val program = UnknownEvent ~> p ++ terminate

    run(program, p)
  }


  def run(pProgram: FlowF[IO, Unit], pProcesses: Process[IO]*): Unit = {
    val app = new CatsApp {
      override val program: ProcessFlow = pProgram

      override val processes: Array[Process[IO]] = pProcesses.toArray
    }
    app.run.unsafeRunSync()
  }

}

object EventDeliverySpec {

  case class QualifiedEvent(pRef: ProcessRef) extends Event

  object UnknownEvent extends Event

  def createProcesses(numOfProcesses: Int, cb: () => Unit): Seq[Process[IO]] = {
    (0 until numOfProcesses).map { i =>
      new Process[IO] {
        override val name: String = s"p-$i"
        override val handle: Receive = {
          case QualifiedEvent(pRef) =>
            if (pRef != ref) eval(throw new RuntimeException(s"unexpected process ref. expected: $ref, actual: $pRef"))
            else eval(cb())
        }
      }
    }
  }
}