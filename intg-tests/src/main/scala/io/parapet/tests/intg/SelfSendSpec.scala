package io.parapet.tests.intg

import cats.effect.{Concurrent, Timer}
import io.parapet.core.Event.Start
import io.parapet.core.{Event, Process}
import io.parapet.tests.intg.SelfSendSpec._
import io.parapet.testutils.{EventStore, IntegrationSpec, TestApp}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

abstract class SelfSendSpec[F[_] : Concurrent : Timer : TestApp]
  extends FlatSpec with IntegrationSpec[F] {

  import dsl._

  val ct: Concurrent[F] = implicitly[Concurrent[F]]

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

    eventStore.await(count, run(ct.pure(Seq(process)))).unsafeRunSync()

    eventStore.size shouldBe count

  }

}

object SelfSendSpec {

  case class Counter(value: Int) extends Event

}