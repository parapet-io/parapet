package io.parapet.tests.intg

import io.parapet.core.{Event, Process}
import io.parapet.tests.intg.ProcessBehaviourSpec._
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

abstract class ProcessBehaviourSpec[F[_]] extends AnyFunSuite with IntegrationSpec[F] {

  import dsl._

  test("switch behaviour") {
    val eventStore = new EventStore[F, Event]
    val process: Process[F] = new Process[F] {

      def uninitialized: Receive = {
        case Init =>
          eval(eventStore.add(ref, Init)) ++ switch(ready)
      }

      def ready: Receive = {
        case Run => eval(eventStore.add(ref, Run))
      }

      override def handle: Receive = uninitialized
    }


    val init = onStart(Seq(Init, Run, Run) ~> process.ref)

    unsafeRun(eventStore.await(3, createApp(ct.pure(Seq(init, process))).run))


    eventStore.get(process.ref) shouldBe Seq(Init, Run, Run)
  }

}

object ProcessBehaviourSpec {

  object Init extends Event

  object Run extends Event

}
