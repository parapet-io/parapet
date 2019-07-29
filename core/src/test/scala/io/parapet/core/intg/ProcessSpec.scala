package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Dsl.WithDsl
import io.parapet.core.Event.{DeadLetter, Envelope, Failure, Start}
import io.parapet.core.exceptions.EventMatchException
import io.parapet.core.intg.ProcessSpec._
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.core.testutils.{EventStore, IntegrationSpec}
import io.parapet.core.{Event, Process, ProcessRef}
import org.scalatest.Matchers.{empty => _, _}
import org.scalatest.OptionValues._
import org.scalatest.WordSpec

class ProcessSpec extends WordSpec with IntegrationSpec with WithDsl[IO] {

  import dsl._

  "A process" when {
    "invoking other process" should {
      "receive response" in {
        val eventStore = new EventStore[Event]

        val process: Process[IO] = new Process[IO] {
          val multiplier = new Multiplier
          override val handle: Receive = {
            case Start => multiplier(ref, Multiply(2, 3))
            case r: Result => eval(eventStore.add(ref, r))
          }
        }
        val processes = Array(process)
        val program = for {
          fiber <- run(processes).start
          _ <- eventStore.awaitSizeOld(1).guaranteeCase(_ => fiber.cancel)
        } yield ()

        program.unsafeRunSync()
        eventStore.size shouldBe 1
        eventStore.get(process.ref).headOption.value should matchPattern {
          case Result(6) =>
        }

      }
    }
  }

  "A process" when {
    "invoking other process fails to match event" should {
      "receive Failure event" in {
        val eventStore = new EventStore[Event]

        val process: Process[IO] = new Process[IO] {
          val multiplier = new Multiplier
          override val handle: Receive = {
            case Start => multiplier(ref, Result(42))
            case r: Result => eval(eventStore.add(ref, r))
            case f: Failure => eval(eventStore.add(ref, f))
          }
        }
        val processes = Array(process)
        val program = for {
          fiber <- run(processes).start
          _ <- eventStore.awaitSizeOld(1).guaranteeCase(_ => fiber.cancel)
        } yield ()

        program.unsafeRunSync()
        eventStore.size shouldBe 1
        eventStore.get(process.ref).headOption.value should matchPattern {
          case Failure(Envelope(process.`ref`, Result(42), Multiplier.ref), _: EventMatchException) =>
        }

      }
    }
  }

  "A process p1 composed with process p2 using `or`" when {
    "p1 can't match event" should {
      "invoke p2" in {
        val eventStore = new EventStore[Event]
        val p1 = new Process[IO] {
          override val handle: Receive = {
            case Start => unit
          }
        }
        val p2 = new Process[IO] {
          override val handle: Receive = {
            case r: Result => eval(eventStore.add(ref, r))
          }
        }

        val composed = p1.or(p2)
        val processes = Array(composed)

        val program = for {
          fiber <- run(processes, Result(42) ~> composed).start
          _ <- eventStore.awaitSizeOld(1).guaranteeCase(_ => fiber.cancel)
        } yield ()

        program.unsafeRunSync()
        eventStore.size shouldBe 1
        eventStore.get(p2.ref).headOption.value should matchPattern {
          case Result(42) =>
        }

      }
    }
  }

  "A two processes p1 and p2 composed using `and`" when {
    "both defined for event X" should {
      "deliver event X" in {
        val eventStore = new EventStore[Event]
        val p1 = new Process[IO] {
          override val handle: Receive = {
            case r: Result => eval(eventStore.add(ref, r))
          }
        }
        val p2 = new Process[IO] {
          override val handle: Receive = {
            case r: Result => eval(eventStore.add(ref, r))
          }
        }

        val composed = p1.and(p2)
        val processes = Array(composed)

        val program = for {
          fiber <- run(processes, Result(42) ~> composed).start
          _ <- eventStore.awaitSizeOld(2).guaranteeCase(_ => fiber.cancel)
        } yield ()

        program.unsafeRunSync()
        eventStore.size shouldBe 2
        eventStore.get(p1.ref).headOption.value should matchPattern {
          case Result(42) =>
        }
        eventStore.get(p2.ref).headOption.value should matchPattern {
          case Result(42) =>
        }
      }
    }
  }

  "A two processes p1 and p2 composed using `and`" when {
    "one isn't defined for event X" should {
      "neither receives event" in {
        val eventStore = new EventStore[DeadLetter]

        val deadLetter = new DeadLetterProcess[IO] {
          val handle: Receive = {
            case f: DeadLetter => eval(eventStore.add(ref, f))
          }
        }
        val p1 = new Process[IO] {
          override val handle: Receive = {
            case Start => unit
          }
        }
        val p2 = new Process[IO] {
          override val handle: Receive = {
            case Start => unit
          }
        }

        val composed = p1.and(p2)
        val processes = Array(composed)
        val program = for {
          fiber <- run(processes, Result(42) ~> composed, Some(deadLetter)).start
          _ <- eventStore.awaitSizeOld(1).guaranteeCase(_ => fiber.cancel)
        } yield ()

        program.unsafeRunSync()
        eventStore.size shouldBe 1

        eventStore.get(deadLetter.ref).headOption.value should matchPattern {
          case DeadLetter(Envelope(TestSystemRef, Result(42), composed.`ref`), _: EventMatchException) =>
        }

      }
    }
  }
}

object ProcessSpec {

  class Multiplier extends Process[IO] {

    import dsl._

    override val ref: ProcessRef = Multiplier.ref

    override val handle: Receive = {
      case Multiply(a, b) => withSender(sender => Result(a * b) ~> sender)
    }
  }

  object Multiplier {
    val ref = ProcessRef("multiplier")
  }

  case class Multiply(a: Int, b: Int) extends Event

  case class Result(res: Int) extends Event

}

