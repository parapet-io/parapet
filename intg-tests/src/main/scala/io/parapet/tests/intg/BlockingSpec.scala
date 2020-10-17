package io.parapet.tests.intg

import io.parapet.core.Event.Start
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Scheduler.SchedulerConfig
import io.parapet.core.{Event, Process, ProcessRef}
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.OptionValues._

abstract class BlockingSpec[F[_]] extends FunSuite with IntegrationSpec[F] {

  import BlockingSpec._
  import dsl._

  test("blocking") {

    val eventStore = new EventStore[F, TestEvent.type]

    val slowRef = ProcessRef("slow")
    val fastRef = ProcessRef("fast")

    val slowProcess = Process.builder[F](_ => {
      case Start => TestEvent ~> fastRef ++ blocking(eval(while (true) {})) ++ TestEvent ~> fastRef
    }).ref(slowRef).name("slow").build

    val fastProcess = Process.builder[F](ref => {
      case TestEvent => eval(eventStore.add(ref, TestEvent))
    }).ref(fastRef).name("fast").build

    unsafeRun(eventStore.await(2, createApp(ct.pure(Seq(slowProcess, fastProcess)), config0 = ParConfig(-1, SchedulerConfig(numberOfWorkers = 1))).run))
    eventStore.get(fastProcess.ref).headOption.value shouldBe TestEvent
  }

}


object BlockingSpec {

  object TestEvent extends Event

}