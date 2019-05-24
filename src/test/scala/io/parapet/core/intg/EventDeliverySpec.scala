package io.parapet.core.intg

import java.util.concurrent.atomic.AtomicInteger

import cats.effect.IO
import io.parapet.core.Parapet._
import io.parapet.core.catsInstances.effect._
import io.parapet.core.catsInstances.flow.{empty => emptyFlow, _}
import io.parapet.core.intg.EventDeliverySpec._
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class EventDeliverySpec extends FlatSpec with IntegrationSpec {

  "Event sent to each process" should "be eventually delivered" in {
    val counter = new AtomicInteger()
    val numOfProcesses = 1000
    val processes =
      createProcesses(numOfProcesses, () => counter.incrementAndGet())
    val program = processes.foldLeft(emptyFlow)((acc, p) => acc ++ QualifiedEvent(p.ref) ~> p)
    run(program ++ terminate, processes: _*)

    counter.get() shouldBe numOfProcesses
  }

  "Unmatched event" should "be ignored" in {
    val p = Process[IO](_ => {
      case Start => emptyFlow
    })

    val program = UnknownEvent ~> p ++ terminate

    run(program, p)
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
            if (pRef != ref)
             eval(throw new RuntimeException(s"unexpected process ref. expected: $ref, actual: $pRef"))
            else eval(cb())
        }
      }
    }
  }
}