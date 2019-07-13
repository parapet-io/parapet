package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Dsl.WithDsl
import io.parapet.core.Event.{DeadLetter, Envelope, Start}
import io.parapet.core.Parapet._
import io.parapet.core.exceptions.{EventDeliveryException, UnknownProcessException}
import io.parapet.core.intg.SchedulerSpec._
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.core.testutils.{EventStore, IntegrationSpec}
import io.parapet.core.{Event, Process}
import io.parapet.implicits._
import org.scalatest.Matchers.{matchPattern, empty => _, _}
import org.scalatest.OptionValues._
import org.scalatest.WordSpec

import scala.concurrent.duration._

class SchedulerSpec extends WordSpec with IntegrationSpec with WithDsl[IO] {

  import effectDsl._
  import flowDsl._


  "Scheduler" when {
    "received task for unknown process" should {
      "send event to deadletter" in {
        val deadLetterEventStore = new EventStore[DeadLetter]
        val deadLetter = new DeadLetterProcess[IO] {
          def handle: Receive = {
            case f: DeadLetter => eval(deadLetterEventStore.add(selfRef, f))
          }
        }

        val unknownProcess = new Process[IO] {
          def handle: Receive = {
            case _ => empty
          }
        }

        val client = new Process[IO] {
          def handle: Receive = {
            case Start => Request ~> unknownProcess
          }
        }

        val processes = Array(client)

        val program = for {
          fiber <- run(empty, processes, Some(deadLetter)).start
          _ <- deadLetterEventStore.awaitSize(1).guaranteeCase(_ => fiber.cancel)

        } yield ()
        program.unsafeRunSync()

        deadLetterEventStore.size shouldBe 1
        deadLetterEventStore.get(deadLetter.selfRef).headOption.value should matchPattern {
          case DeadLetter(Envelope(client.selfRef, Request, unknownProcess.selfRef), _: UnknownProcessException) =>
        }
      }
    }

  }

  "Scheduler" when {
    "process event queue is full" should {
      "send event to deadletter" in {
        val processQueueSize = 1
        val deadLetterEventStore = new EventStore[DeadLetter]
        val deadLetter = new DeadLetterProcess[IO] {
          def handle: Receive = {
            case f: DeadLetter => eval(deadLetterEventStore.add(selfRef, f))
          }
        }

        val slowServer = new Process[IO] {
          override def handle: Receive = {
            case _: NamedRequest => eval(while (true){})
          }
        }

        val client = new Process[IO] {
          override def handle: Receive = {
            case Start => NamedRequest("1") ~> slowServer ++
              delay(5.second, Seq(NamedRequest("2"), NamedRequest("3")) ~> slowServer)
          }
        }

        val processes = Array(client, slowServer)

        val updatedConfig = processQueueSizeLens.set(defaultConfig)(processQueueSize)
        println(updatedConfig)
        val program = for {
          fiber <- run(empty, processes, Some(deadLetter), updatedConfig).start
          _ <- deadLetterEventStore.awaitSize(1).guaranteeCase(_ => fiber.cancel)

        } yield ()
        program.unsafeRunSync()
        deadLetterEventStore.print()
        deadLetterEventStore.size shouldBe 1
        deadLetterEventStore.get(deadLetter.selfRef).headOption.value should matchPattern {
          case DeadLetter(Envelope(client.selfRef, NamedRequest("3"), slowServer.selfRef), _: EventDeliveryException) =>
        }

      }
    }
  }

}

object SchedulerSpec {

  object Request extends Event

  case class NamedRequest(name: String) extends Event

}
