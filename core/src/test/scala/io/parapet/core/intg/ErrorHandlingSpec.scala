package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Dsl.WithDsl
import io.parapet.core.Event._
import io.parapet.core.exceptions.EventHandlingException
import io.parapet.core.intg.ErrorHandlingSpec._
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.core.testutils.{EventStore, IntegrationSpec}
import io.parapet.core.{Event, Process}
import io.parapet.implicits._
import org.scalatest.Matchers.{matchPattern, empty => _, _}
import org.scalatest.OptionValues._
import org.scalatest.WordSpec

class ErrorHandlingSpec extends WordSpec with IntegrationSpec with WithDsl[IO] {

  import effectDsl._
  import flowDsl._

  "System" when {
    "process failed to handle event" should {
      "send Failure event to the sender" in {
        val clientEventStore = new EventStore[Failure]
        val client = new Process[IO] {
          val handle: Receive = {
            case Start => Request ~> faultyServer
            case f: Failure => eval(clientEventStore.add(selfRef, f))
          }
        }

        val processes = Array(client, faultyServer)

        val program = for {
          fiber <- run(empty, processes).start
          _ <- clientEventStore.awaitSize(1).guaranteeCase(_ => fiber.cancel)

        } yield ()
        program.unsafeRunSync()

        clientEventStore.size shouldBe 1
        clientEventStore.get(client.selfRef).headOption.value should matchPattern {
          case Failure(Envelope(client.selfRef, Request, faultyServer.selfRef), _: EventHandlingException) =>
        }
      }
    }
  }

  "System" when {
    "process doesn't have error handling" should {
      "send Failure event to dead letter" in {
        val deadLetterEventStore = new EventStore[DeadLetter]
        val deadLetter = new DeadLetterProcess[IO] {
          val handle: Receive = {
            case f: DeadLetter => eval(deadLetterEventStore.add(selfRef, f))
          }
        }
        val server = new Process[IO] {
          val handle: Receive = {
            case Request => eval(throw new RuntimeException("server is down"))
          }
        }
        val client = new Process[IO] {
          val handle: Receive = {
            case Start => Request ~> server
          }
        }

        val processes = Array(client, server)

        val program = for {
          fiber <- run(empty, processes, Some(deadLetter)).start
          _ <- deadLetterEventStore.awaitSize(1).guaranteeCase(_ => fiber.cancel)

        } yield ()
        program.unsafeRunSync()

        deadLetterEventStore.size shouldBe 1
        deadLetterEventStore.get(deadLetter.selfRef).headOption.value should matchPattern {
          case DeadLetter(Envelope(client.selfRef, Request, server.selfRef), _: EventHandlingException) =>
        }

      }
    }
  }

  "System" when {
    "process failed to handle Failure event" should {
      "send Failure event to dead letter" in {
        val deadLetterEventStore = new EventStore[DeadLetter]
        val deadLetter = new DeadLetterProcess[IO] {
          val handle: Receive = {
            case f: DeadLetter => eval(deadLetterEventStore.add(selfRef, f))
          }
        }
        val server = new Process[IO] {
          val handle: Receive = {
            case Request => eval(throw new RuntimeException("server is down"))
          }
        }
        val client = new Process[IO] {
          val handle: Receive = {
            case Start => Request ~> server
            case _: Failure => eval(throw new RuntimeException("client failed to handle error"))
          }
        }

        val processes = Array(client, server)

        val program = for {
          fiber <- run(empty, processes, Some(deadLetter)).start
          _ <- deadLetterEventStore.awaitSize(1).guaranteeCase(_ => fiber.cancel)

        } yield ()
        program.unsafeRunSync()

        deadLetterEventStore.size shouldBe 1
        deadLetterEventStore.get(deadLetter.selfRef).headOption.value should matchPattern {
          case DeadLetter(Envelope(client.selfRef, Request, server.selfRef), _: EventHandlingException) =>
        }
      }
    }
  }

}

object ErrorHandlingSpec {

  val faultyServer: Process[IO] = new Process[IO] {

    import effectDsl._

    val handle: Receive = {
      case Request => eval(throw new RuntimeException("server is down"))
    }
  }

  object Request extends Event

}
