package io.parapet.tests.intg

import io.parapet.core.Event.{DeadLetter, Envelope, Start}
import io.parapet.core.Parapet._
import io.parapet.core.exceptions.{EventDeliveryException, UnknownProcessException}
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.core.{Event, Process}
import io.parapet.tests.intg.SchedulerSpec._
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.Matchers._
import org.scalatest.OptionValues._
import org.scalatest.WordSpec

import scala.concurrent.duration._

abstract class SchedulerSpec[F[_]] extends WordSpec with IntegrationSpec[F] {

  import dsl._

  "Scheduler" when {
    "received task for unknown process" should {
      "send event to deadletter" in {
        val eventStore = new EventStore[F, DeadLetter]
        val deadLetter = new DeadLetterProcess[F] {
          def handle: Receive = {
            case f: DeadLetter => eval(eventStore.add(ref, f))
          }
        }

        val unknownProcess = new Process[F] {
          def handle: Receive = {
            case _ => unit
          }
        }

        val client = new Process[F] {
          def handle: Receive = {
            case Start => Request ~> unknownProcess
          }
        }

        unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(client)), Some(ct.pure(deadLetter))).run))

        eventStore.size shouldBe 1
        eventStore.get(deadLetter.ref).headOption.value should matchPattern {
          case DeadLetter(Envelope(client.`ref`, Request, unknownProcess.`ref`), _: UnknownProcessException) =>
        }
      }
    }

  }

  // todo unstable
  "Scheduler" when {
    "process event queue is full" should {
      "send event to deadletter" ignore {
        val processQueueSize = 1
        val eventStore = new EventStore[F, DeadLetter]
        val deadLetter = new DeadLetterProcess[F] {
          def handle: Receive = {
            case f: DeadLetter => eval(eventStore.add(ref, f))
          }
        }

        val slowServer = new Process[F] {
          override def handle: Receive = {
            case _: NamedRequest => eval(while (true) {})
          }
        }

        val client = new Process[F] {
          override def handle: Receive = {
            case Start => NamedRequest("1") ~> slowServer ++
              delay(5.seconds) ++ NamedRequest("2") ~> slowServer ++ NamedRequest("3") ~> slowServer
          }
        }

        val config = processBufferSizeLens.set(ParConfig.default)(processQueueSize)

        unsafeRun(eventStore.await(1,
          createApp(ct.pure(Seq(client, slowServer)), Some(ct.pure(deadLetter)), config).run))

        eventStore.size shouldBe 1
        eventStore.get(deadLetter.ref).headOption.value should matchPattern {
          case DeadLetter(Envelope(client.`ref`, NamedRequest("3"), slowServer.`ref`), _: EventDeliveryException) =>
        }

      }
    }
  }

}

object SchedulerSpec {

  object Request extends Event

  case class NamedRequest(name: String) extends Event

}
