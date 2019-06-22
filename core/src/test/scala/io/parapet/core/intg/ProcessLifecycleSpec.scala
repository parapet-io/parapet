package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Dsl.WithDsl
import io.parapet.core.Event._
import io.parapet.core.intg.ProcessLifecycleSpec._
import io.parapet.core.testutils.{EventStore, IntegrationSpec}
import io.parapet.core.{Event, Process}
import io.parapet.implicits._
import org.scalatest.FlatSpec
import org.scalatest.Matchers._


class ProcessLifecycleSpec extends FlatSpec with IntegrationSpec with WithDsl[IO] {

  import effectDsl._

  "Start event" should "be delivered before client events" in {
    val expectedEventsCount = 2
    val eventStore = new EventStore[Event]
    val process = new Process[IO] {
      val handle: Receive = {
        case Start => eval(eventStore.add(selfRef, Start))
        case TestEvent => eval(eventStore.add(selfRef, TestEvent))
      }
    }

    val sendEvent = TestEvent ~> process
    val processes = Array(process)
    val program = for {
      fiber <- run(sendEvent, processes).start
      _ <- eventStore.awaitSize(expectedEventsCount).guaranteeCase(_ => fiber.cancel)
    } yield ()

    program.unsafeRunSync()

    eventStore.size shouldBe expectedEventsCount
    eventStore.get(process.selfRef) shouldBe Seq(Start, TestEvent)

  }

  "Stop" should "be delivered last" in {
    val domainEventsCount = 1
    val totalEventsCount = 2 // domainEventsCount + Stop
    val eventStore = new EventStore[Event]
    val process = new Process[IO] {
      val handle: Receive = {
        case TestEvent => eval(eventStore.add(selfRef, TestEvent))
        case Stop => eval(eventStore.add(selfRef, Stop))
      }
    }

    val sendEvent = TestEvent ~> process
    val processes = Array(process)
    val program = for {
      fiber <- run(sendEvent, processes).start
      _ <- eventStore.awaitSize(domainEventsCount).guaranteeCase(_ => fiber.cancel)
    } yield ()

    program.unsafeRunSync()

    eventStore.size shouldBe totalEventsCount
    eventStore.get(process.selfRef) shouldBe Seq(TestEvent, Stop)
  }

}

object ProcessLifecycleSpec {

  object TestEvent extends Event

}
