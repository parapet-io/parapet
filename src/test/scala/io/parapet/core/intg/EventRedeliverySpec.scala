package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Parapet._
import io.parapet.core.catsInstances.effect._
import io.parapet.core.catsInstances.flow._
import io.parapet.core.intg.EventRedeliverySpec._
import org.scalatest.{FlatSpec, Matchers, OptionValues}

class EventRedeliverySpec extends FlatSpec with Matchers with OptionValues {

  "Failed events" should "be redelivered n times" in {
    var deliveryCount = 0
    val p = Process[IO] {
      case TextEvent =>
        eval {
          deliveryCount = deliveryCount + 1
        } ++ suspend(IO.raiseError(new RuntimeException("unexpected error")))
    }

    val program = TextEvent ~> p ++ terminate
    run(program, p)

    deliveryCount shouldBe 6
  }

  "'Failure' event" should "be delivered after maximum redelivery attempts has been reached" in {
    var failedEvent = None: Option[Failure]
    val p = Process[IO] {
      case TextEvent => suspend(IO.raiseError(new RuntimeException("unexpected error")))
      case f@Failure(_, _) => eval {
        failedEvent = Some(f)
      }
    }
    val program = TextEvent ~> p ++ terminate
    run(program, p)
    failedEvent.value.error.getMessage shouldBe "unexpected error"
    failedEvent.value.event shouldBe TextEvent
  }

  "Error during Failure event handling" should "not be propagated" in {
    val p = Process[IO] {
      case TextEvent => suspend(IO.raiseError(new RuntimeException("unexpected error")))
      case Failure(_, _) => suspend(IO.raiseError(new RuntimeException("failed to process 'failure' event")))
    }
    val program = TextEvent ~> p ++ terminate
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

object EventRedeliverySpec {

  case object TextEvent extends Event

}
