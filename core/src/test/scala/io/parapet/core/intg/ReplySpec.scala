package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Dsl.WithDsl
import io.parapet.core.Event._
import io.parapet.core.intg.ReplySpec._
import io.parapet.core.testutils.{EventStore, IntegrationSpec}
import io.parapet.core.{Event, Process}
import io.parapet.implicits._
import org.scalatest.FlatSpec
import org.scalatest.Matchers.{empty => _, _}
import org.scalatest.OptionValues._

class ReplySpec extends FlatSpec with IntegrationSpec with WithDsl[IO] {

  import effectDsl._
  import flowDsl._

  "Reply" should "send send event to the sender" in {
    val clientEventStore = new EventStore[Event]
    val server = new Process[IO] {
      def handle: Receive = {
        case Request => reply(sender => Response ~> sender)
      }
    }

    val client = new Process[IO] {
      def handle: Receive = {
        case Start => Request ~> server
        case Response => eval(clientEventStore.add(selfRef, Response))
      }
    }

    val processes = Array(client, server)

    val program = for {
      fiber <- run(empty, processes).start
      _ <- clientEventStore.awaitSize(1).guaranteeCase(_ => fiber.cancel)
    } yield ()

    program.unsafeRunSync()

    clientEventStore.size shouldBe 1
    clientEventStore.get(client.selfRef).headOption.value shouldBe Response


  }

}

object ReplySpec {

  object Request extends Event

  object Response extends Event

}
