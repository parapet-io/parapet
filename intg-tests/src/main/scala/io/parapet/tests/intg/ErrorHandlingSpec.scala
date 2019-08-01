package io.parapet.tests.intg

import io.parapet.core.Event._
import io.parapet.core.exceptions.EventHandlingException
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.core.{Event, Process}
import io.parapet.tests.intg.ErrorHandlingSpec._
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.Matchers._
import org.scalatest.OptionValues._
import org.scalatest.WordSpec

abstract class ErrorHandlingSpec[F[_]] extends WordSpec with IntegrationSpec[F] {

  import dsl._

  "System" when {
    "process failed to handle event" should {
      "send Failure event to the sender" in {
        val clientEventStore = new EventStore[F, Failure]

        val faultyServer = createFaultyServer[F]

        val client = new Process[F] {
          def handle: Receive = {
            case Start => Request ~> faultyServer
            case f: Failure => eval(clientEventStore.add(ref, f))
          }
        }

        clientEventStore.await(1, run(ct.pure(Seq(client, faultyServer)))).unsafeRunSync()

        clientEventStore.size shouldBe 1
        clientEventStore.get(client.ref).headOption.value should matchPattern {
          case Failure(Envelope(client.`ref`, Request, faultyServer.`ref`), _: EventHandlingException) =>
        }
      }
    }
  }

  "System" when {
    "process doesn't have error handling" should {
      "send Failure event to dead letter" in {
        val deadLetterEventStore = new EventStore[F, DeadLetter]
        val deadLetter = new DeadLetterProcess[F] {
          def handle: Receive = {
            case f: DeadLetter => eval(deadLetterEventStore.add(ref, f))
          }
        }
        val server = new Process[F] {
          def handle: Receive = {
            case Request => eval(throw new RuntimeException("server is down"))
          }
        }
        val client = new Process[F] {
          def handle: Receive = {
            case Start => Request ~> server
          }
        }

        deadLetterEventStore.await(1, run(ct.pure(Seq(client, server)), Some(ct.pure(deadLetter)))).unsafeRunSync()

        deadLetterEventStore.size shouldBe 1
        deadLetterEventStore.get(deadLetter.ref).headOption.value should matchPattern {
          case DeadLetter(Envelope(client.`ref`, Request, server.`ref`), _: EventHandlingException) =>
        }

      }
    }
  }

  "System" when {
    "process failed to handle Failure event" should {
      "send Failure event to dead letter" in {
        val deadLetterEventStore = new EventStore[F, DeadLetter]
        val deadLetter = new DeadLetterProcess[F] {
          def handle: Receive = {
            case f: DeadLetter => eval(deadLetterEventStore.add(ref, f))
          }
        }
        val server = new Process[F] {
          def handle: Receive = {
            case Request => eval(throw new RuntimeException("server is down"))
          }
        }
        val client = new Process[F] {
          def handle: Receive = {
            case Start => Request ~> server
            case _: Failure => eval(throw new RuntimeException("client failed to handle error"))
          }
        }

        deadLetterEventStore.await(1, run(ct.pure(Seq(client, server)), Some(ct.pure(deadLetter)))).unsafeRunSync()

        deadLetterEventStore.size shouldBe 1
        deadLetterEventStore.get(deadLetter.ref).headOption.value should matchPattern {
          case DeadLetter(Envelope(client.`ref`, Request, server.`ref`), _: EventHandlingException) =>
        }
      }
    }
  }

}

object ErrorHandlingSpec {

  def createFaultyServer[F[_]]: Process[F] = new Process[F] {

    import dsl._

    def handle: Receive = {
      case Request => eval(throw new RuntimeException("server is down"))
    }
  }

  object Request extends Event

}
