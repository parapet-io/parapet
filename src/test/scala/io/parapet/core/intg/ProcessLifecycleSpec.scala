package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Parapet._
import io.parapet.core.catsInstances.flow._
import io.parapet.core.intg.ProcessLifecycleSpec._
import io.parapet.core.testutils.EventStoreProcess
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import org.scalatest.OptionValues._

class ProcessLifecycleSpec extends FlatSpec with IntegrationSpec {

  "Start event" should "be delivered before client events" in {
    val p1EventStore = new EventStoreProcess(enableSystemEvents = true)
    val p2EventStore = new EventStoreProcess(enableSystemEvents = true)
    val p1 = Process[IO](_ => {
      case e => p1EventStore(e)
    })
    val p2 = Process[IO](_ => {
      case e => p2EventStore(e)
    })

    val program = TestEvent ~> p1 ++ TestEvent ~> p2 ++ terminate

    run(program, p1, p2)

    // start event must be delivered only once per each process
    p1EventStore.events.count(e => e == Start) shouldBe 1
    p2EventStore.events.count(e => e == Start) shouldBe 1

    p1EventStore.events.headOption.value shouldBe Start
    p2EventStore.events.headOption.value shouldBe Start
  }

  "Stop" should "be last delivered event" in  {
    val p1EventStore = new EventStoreProcess(enableSystemEvents = true)
    val p2EventStore = new EventStoreProcess(enableSystemEvents = true)
    val p1 = Process[IO](_ => {
      case e => p1EventStore(e)
    })
    val p2 = Process[IO](_ => {
      case e => p2EventStore(e)
    })

    val program = TestEvent ~> p1 ++ TestEvent ~> p2 ++ terminate

    run(program, p1, p2)

    p1EventStore.events.lastOption.value shouldBe Stop
    p2EventStore.events.lastOption.value shouldBe Stop

    p1EventStore.events.count(e => e == Stop) shouldBe 1
    p2EventStore.events.count(e => e == Stop) shouldBe 1
  }

}

object ProcessLifecycleSpec {

  object TestEvent extends Event

}
