package io.parapet.tests.intg.scheduler

import io.parapet.core.Events.{DeadLetter, Start}
import io.parapet.core.Parapet.*
import io.parapet.core.Process
import io.parapet.core.exceptions.{EventDeliveryException, UnknownProcessException}
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.tests.intg.IntegrationSpec
import io.parapet.tests.intg.scheduler.SchedulerSpec.*
import io.parapet.testutils.EventStore
import io.parapet.{Envelope, Event}
import org.scalatest.OptionValues.*
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*

abstract class SchedulerSpec[F[_]] extends AnyWordSpec with IntegrationSpec[F] {

  import dsl.*

  "Scheduler" when {
    "received task for unknown process" should {
      "send event to deadletter" in {
        val eventStore = new EventStore[F, DeadLetter]
        val deadLetter = new DeadLetterProcess[F] {
          def handle: Receive = { case f: DeadLetter =>
            eval(eventStore.add(ref, f))
          }
        }

        val unknownProcess = new Process[F, Event, Event] {
          def handle: Receive = { case _ =>
            unit
          }
        }

        val client = new Process[F, Event, Event] {
          def handle: Receive = { case Start =>
            Request ~> unknownProcess
          }
        }

        unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(client)), Some(ct.pure(deadLetter))).run))

        eventStore.size shouldBe 1
        eventStore.get(deadLetter.ref).headOption.value should matchPattern {
          case DeadLetter(Envelope(client.`ref`, Request, unknownProcess.`ref`, _), _: UnknownProcessException) =>
        }
      }
    }

  }

  // todo unstable
  "Scheduler" when {
    "process event queue is full" should {
      "send event to deadletter" ignore {
        val processQueueSize = 1
        val eventStore       = new EventStore[F, DeadLetter]
        val deadLetter       = new DeadLetterProcess[F] {
          def handle: Receive = { case f: DeadLetter =>
            eval(eventStore.add(ref, f))
          }
        }

        val slowServer = new Process[F, Event, Event] {
          override def handle: Receive = { case _: NamedRequest =>
            eval(while (true) {})
          }
        }

        val client = new Process[F, Event, Event] {
          override def handle: Receive = { case Start =>
            NamedRequest("1") ~> slowServer ++
              delay(5.seconds) ++ NamedRequest("2") ~> slowServer ++ NamedRequest("3") ~> slowServer
          }
        }

        val config = ParConfig.default.withProcessBufferSize(processQueueSize)

        unsafeRun(
          eventStore.await(1, createApp(ct.pure(Seq(client, slowServer)), Some(ct.pure(deadLetter)), config).run)
        )

        eventStore.size shouldBe 1
        eventStore.get(deadLetter.ref).headOption.value should matchPattern {
          case DeadLetter(Envelope(client.`ref`, NamedRequest("3"), slowServer.`ref`, _), _: EventDeliveryException) =>
        }

      }
    }
  }

}

object SchedulerSpec {

  object Request extends Event

  case class NamedRequest(name: String) extends Event

}
