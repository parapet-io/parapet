package io.parapet.tests.intg

import cats.effect.{Concurrent, Timer}
import io.parapet.core.{Event, Process}
import io.parapet.tests.intg.ProcessBehaviourSpec._
import io.parapet.testutils.{EventStore, IntegrationSpec, TestApp}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

abstract class ProcessBehaviourSpec[F[_] : Concurrent : Timer : TestApp]
  extends FunSuite with IntegrationSpec[F] {

  import dsl._

  val ct: Concurrent[F] = implicitly[Concurrent[F]]

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

    eventStore.await(3, run(ct.pure(Seq(init, process)))).unsafeRunSync()


    eventStore.get(process.ref) shouldBe Seq(Init, Run, Run)
  }

}

object ProcessBehaviourSpec {

  object Init extends Event

  object Run extends Event

}
