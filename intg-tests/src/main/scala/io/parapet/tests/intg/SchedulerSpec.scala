package io.parapet.tests.intg

import cats.effect.{Concurrent, Timer}
import io.parapet.core.Event.{DeadLetter, Envelope, Start}
import io.parapet.core.Parapet._
import io.parapet.core.exceptions.{EventDeliveryException, UnknownProcessException}
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.core.{Event, Process}
import io.parapet.tests.intg.SchedulerSpec._
import io.parapet.testutils.{EventStore, IntegrationSpec, TestApp}
import org.scalatest.Matchers._
import org.scalatest.OptionValues._
import org.scalatest.WordSpec

import scala.concurrent.duration._

abstract class SchedulerSpec[F[_] : Concurrent : Timer : TestApp] extends WordSpec with IntegrationSpec[F] {

  import dsl._

  val ct: Concurrent[F] = implicitly[Concurrent[F]]

  "Scheduler" when {
    "received task for unknown process" should {
      "send event to deadletter" in {
        val deadLetterEventStore = new EventStore[F, DeadLetter]
        val deadLetter = new DeadLetterProcess[F] {
          def handle: Receive = {
            case f: DeadLetter => eval(deadLetterEventStore.add(ref, f))
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

        deadLetterEventStore.await(1, run(ct.pure(Seq(client)), Some(ct.pure(deadLetter)))).unsafeRunSync()

        deadLetterEventStore.size shouldBe 1
        deadLetterEventStore.get(deadLetter.ref).headOption.value should matchPattern {
          case DeadLetter(Envelope(client.`ref`, Request, unknownProcess.`ref`), _: UnknownProcessException) =>
        }
      }
    }

  }

  "Scheduler" when {
    "process event queue is full" should {
      "send event to deadletter" in {
        val processQueueSize = 1
        val deadLetterEventStore = new EventStore[F, DeadLetter]
        val deadLetter = new DeadLetterProcess[F] {
          def handle: Receive = {
            case f: DeadLetter => eval(deadLetterEventStore.add(ref, f))
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
              delay(5.second, Seq(NamedRequest("2"), NamedRequest("3")) ~> slowServer)
          }
        }


        val config = processQueueSizeLens.set(defaultConfig)(processQueueSize)
        deadLetterEventStore.await(1,
          run(ct.pure(Seq(client, slowServer)), Some(ct.pure(deadLetter)), config)).unsafeRunSync()

        deadLetterEventStore.print()
        deadLetterEventStore.size shouldBe 1
        deadLetterEventStore.get(deadLetter.ref).headOption.value should matchPattern {
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
