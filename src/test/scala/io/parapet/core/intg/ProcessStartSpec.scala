package io.parapet.core.intg

import java.util.concurrent.CopyOnWriteArrayList

import cats.effect.IO
import io.parapet.core.Parapet._
import io.parapet.core.catsInstances.effect._
import io.parapet.core.catsInstances.flow._
import io.parapet.core.intg.ProcessStartSpec._
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

// todo fails:
//Expected :ListBuffer(Start, Start, io.parapet.core.intg.ProcessStartSpec$TestEvent$@748741cb, io.parapet.core.intg.ProcessStartSpec$TestEvent$@748741cb, Stop, Stop)
//Actual   :Buffer(Start, io.parapet.core.intg.ProcessStartSpec$TestEvent$@748741cb, Start, io.parapet.core.intg.ProcessStartSpec$TestEvent$@748741cb, Stop, Stop)
class ProcessStartSpec extends FlatSpec {

  "Start event" should "be delivered before client events" in {
    val events = new CopyOnWriteArrayList[Event]()
    val p1 = Process[IO] {
      case e => eval(events.add(e))
    }
    val p2 = Process[IO] {
      case e => eval(events.add(e))
    }

    val program = TestEvent ~> p1 ++ TestEvent ~> p2 ++ terminate

    run(program, p1, p2)

    events.asScala shouldBe ListBuffer(Start, Start, TestEvent, TestEvent, Stop, Stop)
  }
  def run(pProgram: FlowF[IO, Unit], pProcesses: Process[IO]*): Unit = {
    val app = new CatsApp {
      override val program: ProcessFlow = pProgram

      override val processes: Array[Process[IO]] = pProcesses.toArray
    }
    app.run.unsafeRunSync()
  }

}

object ProcessStartSpec {

  object TestEvent extends Event

}
