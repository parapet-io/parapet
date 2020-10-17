package io.parapet.tests.intg

import java.util.concurrent.atomic.AtomicInteger

import io.parapet.core.Event.Start
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Scheduler.SchedulerConfig
import io.parapet.core.{Event, Process, ProcessRef}
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.OptionValues._

import scala.concurrent.duration._
import scala.util.Random

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


  test("multiple blocking") {
    val eventStore = new EventStore[F, Event]
    val total = 10
    val count = new AtomicInteger()

    val process = Process(ref => {
      case Start =>
        (0 until total).map(_ => {
          blocking(delay((1 + Random.nextInt(3)).seconds) ++ eval(count.incrementAndGet()))
        }).reduce(_ ++ _) ++ Done ~> ref

      case Done => eval(eventStore.add(ref, NumEvent(count.get())))
    })


    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(process))).run))
    eventStore.get(process.ref) shouldBe Seq(NumEvent(10))

  }


}


object BlockingSpec {

  object TestEvent extends Event

  object Done extends Event

  case class NumEvent(i: Int) extends Event


}