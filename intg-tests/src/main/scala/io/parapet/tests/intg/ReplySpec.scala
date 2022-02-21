package io.parapet.tests.intg

import io.parapet.Event
import io.parapet.core.Events._
import io.parapet.core.Process
import io.parapet.tests.intg.ReplySpec._
import io.parapet.testutils.EventStore
import org.scalatest.OptionValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

abstract class ReplySpec[F[_]] extends AnyFlatSpec with IntegrationSpec[F] {

  import dsl._

  "Reply" should "send send event to the sender" in {
    val clientEventStore = new EventStore[F, Event]
    val server = new Process[F] {
      def handle: Receive = {
        case Request => withSender(sender => Response ~> sender)
      }
    }

    val client = new Process[F] {
      def handle: Receive = {
        case Start => Request ~> server
        case Response => eval(clientEventStore.add(ref, Response))
      }
    }

    unsafeRun(clientEventStore.await(1, createApp(ct.pure(Seq(client, server))).run))


    clientEventStore.size shouldBe 1
    clientEventStore.get(client.ref).headOption.value shouldBe Response

  }

}

object ReplySpec {

  object Request extends Event

  object Response extends Event

}
