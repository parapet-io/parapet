package io.parapet.tests.intg

import io.parapet.Event
import io.parapet.core.Events.Start
import io.parapet.core.Process
import io.parapet.tests.intg.SelfSendSpec._
import io.parapet.testutils.EventStore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

abstract class SelfSendSpec[F[_]] extends AnyFlatSpec with IntegrationSpec[F] {

  import dsl._

  "Process" should "be able to send event to itself" in {
    val eventStore = new EventStore[F, Counter]
    val count = 11000

    val process = new Process[F] {
      def handle: Receive = {
        case Start => Counter(count) ~> ref
        case c@Counter(i) =>
          if (i == 0) unit
          else eval(eventStore.add(ref, c)) ++ Counter(i - 1) ~> ref
      }
    }

    unsafeRun(eventStore.await(count, createApp(ct.pure(Seq(process))).run))

    eventStore.size shouldBe count

  }

}

object SelfSendSpec {

  case class Counter(value: Int) extends Event

}