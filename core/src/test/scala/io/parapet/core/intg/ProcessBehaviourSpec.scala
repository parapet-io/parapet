package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Dsl.{DslF, WithDsl}
import io.parapet.core.testutils.{EventStore, IntegrationSpec}
import io.parapet.core.{Event, Process}
import org.scalatest.FunSuite
import io.parapet.core.intg.ProcessBehaviourSpec._
import io.parapet.implicits._
import org.scalatest.Matchers._

class ProcessBehaviourSpec extends FunSuite with WithDsl[IO] with IntegrationSpec {

  import dsl._


  test("switch behaviour") {
    val eventStore = new EventStore[Event]
    val p: Process[IO] = new Process[IO] {

      def uninitialized: Receive = {
        case Init =>
          eval(eventStore.add(selfRef, Init)) ++ switch(ready)
      }

      def ready: Receive = {
        case Run => eval(eventStore.add(selfRef, Run))
      }

      override def handle: Receive = uninitialized
    }

    val processes = Array(p)

    val init: DslF[IO, Unit] = Seq(Init, Run, Run) ~> p.selfRef

    val program = for {
      fiber <- run(processes, init).start
      _ <- eventStore.awaitSizeOld(3).guaranteeCase(_ => fiber.cancel)

    } yield ()
    program.unsafeRunSync()

    eventStore.get(p.selfRef) shouldBe Seq(Init, Run, Run)
  }

}

object ProcessBehaviourSpec {

  object Init extends Event

  object Run extends Event

}
