package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Dsl.WithDsl
import io.parapet.core.intg.SelfSendSpec._
import io.parapet.core.testutils.{EventStore, IntegrationSpec}
import io.parapet.core.{Event, Process}
import io.parapet.implicits._
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class SelfSendSpec extends FlatSpec with IntegrationSpec with WithDsl[IO]{

  import effectDsl._
  import flowDsl._

  "Process" should "be able to send event to itself" in {
    val eventStore = new EventStore[Counter]
    val count = 11000

    val process = new Process[IO] {
      val handle: Receive = {
        case c@Counter(i) =>
          if (i == 0) empty
          else eval(eventStore.add(selfRef, c)) ++ Counter(i - 1) ~> selfRef
      }
    }

   val processes = Array(process)

    val program = for {
      fiber <- run(Counter(count) ~> process, processes).start
      _ <- eventStore.awaitSize(count).guaranteeCase(_ => fiber.cancel)
    } yield ()

    program.unsafeRunSync()

    eventStore.size shouldBe count

  }

}

object SelfSendSpec {

  case class Counter(value: Int) extends Event

}