package io.parapet.tests.intg

import io.parapet.core.Event._
import io.parapet.core.exceptions.EventMatchException
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.core.{Event, Process, ProcessRef}
import io.parapet.tests.intg.EventDeliverySpec._
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.OptionValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

abstract class EventDeliverySpec[F[_]] extends AnyFlatSpec with IntegrationSpec[F] {

  import dsl._

  "Event" should "be sent to correct process" in {
    val eventStore = new EventStore[F, QualifiedEvent]
    val numOfProcesses = 10
    val processes =
      createProcesses[F](numOfProcesses, eventStore)

    val events = processes.map(p => p.ref -> QualifiedEvent(p.ref)).toMap

    val init = onStart(events.foldLeft(unit) {
      case (acc, (pRef, event)) => acc ++ event ~> pRef
    })

    unsafeRun(eventStore.await(10, createApp(ct.pure(processes :+ init)).run))

    eventStore.size shouldBe events.size

    events.foreach {
      case (pRef, event) =>
        eventStore.get(pRef).size shouldBe 1
        eventStore.get(pRef).headOption.value shouldBe event
    }

  }


  "Event" should "be evaluated lazily" in {

    var i = 0
    val eventStore = new EventStore[F, NumEvent]
    val consumer = Process[F](ref => {
      case e@NumEvent(_) => eval(eventStore.add(ref, e))
    })

    val producer = Process[F](_ => {
      case Start => eval {
        i = i + 1
      } ++ send(NumEvent(i), consumer.ref)
    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(producer, consumer))).run))

    eventStore.get(consumer.ref) shouldBe Seq(NumEvent(1))
  }

  "Event" should "be evaluated lazily using for_comprehension" in {

    var i = 0
    val eventStore = new EventStore[F, NumEvent]
    val consumer = Process[F](ref => {
      case e@NumEvent(_) => eval(eventStore.add(ref, e))
    })

    val producer = Process[F](_ => {
      case Start =>
        for {
          v <- eval {
            i = i + 1
            i
          }
          _ <- NumEvent(v) ~> consumer.ref
        } yield ()
    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(producer, consumer))).run))

    eventStore.get(consumer.ref) shouldBe Seq(NumEvent(1))
  }

  "Event" should "be evaluated lazily using syntax" in {

    var i = 0
    val eventStore = new EventStore[F, NumEvent]
    val consumer = Process[F](ref => {
      case e@NumEvent(_) => eval(eventStore.add(ref, e))
    })

    val producer = Process[F](_ => {
      case Start =>
        eval {
          i = i + 1
        } ++ NumEvent(i) ~> consumer.ref
    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(producer, consumer))).run))

    eventStore.get(consumer.ref) shouldBe Seq(NumEvent(1))
  }

  "Unmatched event" should "be sent to deadletter" in {
    val eventStore = new EventStore[F, DeadLetter]
    val deadLetter = new DeadLetterProcess[F] {
      def handle: Receive = {
        case f: DeadLetter => eval(eventStore.add(ref, f))
      }
    }

    val server = new Process[F] {
      def handle: Receive = {
        case Start => unit
      }
    }

    val client = new Process[F] {
      def handle: Receive = {
        case Start => UnknownEvent ~> server
      }
    }

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(client, server)), Some(ct.pure(deadLetter))).run))

    eventStore.size shouldBe 1
    eventStore.get(deadLetter.ref).headOption.value should matchPattern {
      case DeadLetter(Envelope(client.`ref`, UnknownEvent, server.`ref`), _: EventMatchException) =>
    }
  }

}

object EventDeliverySpec {

  case class QualifiedEvent(pRef: ProcessRef) extends Event

  object UnknownEvent extends Event

  case class NumEvent(i: Int) extends Event

  def createProcesses[F[_]](numOfProcesses: Int, eventStore: EventStore[F, QualifiedEvent]): Seq[Process[F]] = {
    (0 until numOfProcesses).map { i =>
      new Process[F] {

        import dsl._

        override val name: String = s"p-$i"

        override def handle: Receive = {
          case e: QualifiedEvent => eval(eventStore.add(ref, e))
        }
      }
    }
  }
}