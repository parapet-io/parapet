package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.intg.SelfSendSpec._
import io.parapet.core.testutils.{EventStore, IntegrationSpec}
import io.parapet.core.{Event, Process}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class SelfSendSpec extends FlatSpec with IntegrationSpec {

  import dsl._

  "Process" should "be able to send event to itself" in {
    val eventStore = new EventStore[Counter]
    val count = 11000

    val process = new Process[IO] {
      def handle: Receive = {
        case c@Counter(i) =>
          if (i == 0) unit
          else eval(eventStore.add(ref, c)) ++ Counter(i - 1) ~> ref
      }
    }

   val processes = Array(process)

    val program = for {
      fiber <- run(processes, Counter(count) ~> process).start
      _ <- eventStore.awaitSizeOld(count).guaranteeCase(_ => fiber.cancel)
    } yield ()

    program.unsafeRunSync()

    eventStore.size shouldBe count

  }

}

object SelfSendSpec {

  case class Counter(value: Int) extends Event

}