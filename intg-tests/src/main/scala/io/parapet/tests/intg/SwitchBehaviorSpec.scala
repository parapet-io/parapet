package io.parapet.tests.intg

import io.parapet.core.Dsl.DslF
import io.parapet.core.{Channel, Event, Process, ProcessRef}
import io.parapet.tests.intg.SwitchBehaviorSpec._
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.FunSuite
import org.scalatest.Matchers._

abstract class SwitchBehaviorSpec[F[_]] extends FunSuite with IntegrationSpec[F] {
  self =>

  import dsl._


  test("switch a process  should ") {
    val eventStore = new EventStore[F, Event]

    def saveEvent1: ProcessRef => PartialFunction[Event, DslF[F, Unit]] = ref => {
      case Event1 => eval(eventStore.add(ref, Event1))
    }

    def saveEvent2: ProcessRef => PartialFunction[Event, DslF[F, Unit]] = ref => {
      case Event2 => eval(eventStore.add(ref, Event2))
    }

    val p1Ref = ProcessRef("p1")
    val p2Ref = ProcessRef("p2")

    val p1 = new TestProcess[F](saveEvent1(p1Ref), eventStore, p1Ref)
    val p2 = new TestProcess[F](saveEvent2(p2Ref), eventStore, p2Ref)

    val p = p1.or(p2)
    val ch = new Channel[F]()

    val test = onStart {
      // we need a channel here to introduce synchronization boundary, this approach can be improved in future releases
      register(TestSystemRef, ch) ++ ch.send(Switch(State3), p2.ref, _ => Event3 ~> p)
    }

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(test, p, p2))).run))
    eventStore.get(p2.ref) shouldBe Seq(Event3)

  }


}

object SwitchBehaviorSpec {

  class TestProcess[F[_]](initial: PartialFunction[Event, DslF[F, Unit]],
                          es: EventStore[F, Event],
                          override val ref: ProcessRef
                         ) extends Process[F] {
    self =>

    import dsl._

    var state: State = _

    def handleEvent1: PartialFunction[Event, DslF[F, Unit]] = {
      case Event1 => eval(es.add(ref, Event1))
    }

    def handleEvent2: PartialFunction[Event, DslF[F, Unit]] = {
      case Event2 => eval(es.add(ref, Event2))
    }


    def handleEvent3: PartialFunction[Event, DslF[F, Unit]] = {
      case Event3 => eval(es.add(ref, Event3))
    }

    def switch(s: State): DslF[F, Unit] =
      s match {
        case State1 => eval(state = State1) ++ switch(onSwitch.orElse(handleEvent1))
        case State2 => eval(state = State2) ++ switch(onSwitch.orElse(handleEvent2))
        case State3 => eval(state = State3) ++ switch(onSwitch.orElse(handleEvent3))
      }

    def onSwitch: Receive = {
      case Switch(s) => switch(s) ++ withSender(Ok ~> _)
    }


    override def handle: Receive = onSwitch.orElse(initial)
  }

  sealed trait State
  case object State1 extends State
  case object State2 extends State
  case object State3 extends State

  case class Switch(e: State) extends Event

  object Event1 extends Event
  object Event2 extends Event
  object Event3 extends Event

  object Ok extends Event

}
