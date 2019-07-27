package io.parapet.core.intg

import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.IO
import cats.syntax.flatMap._
import io.parapet.core.Dsl.DslF
import io.parapet.core.Event._
import io.parapet.core.intg.ProcessLifecycleSpec._
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.core.testutils.{EventStore, IntegrationSpec}
import io.parapet.core.{Event, Process, ProcessRef}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import org.scalatest.OptionValues._

import scala.concurrent.duration._

class ProcessLifecycleSpec extends FlatSpec with IntegrationSpec {

  import dsl._

  "Start event" should "be delivered before client events" in {
    val expectedEventsCount = 2
    val eventStore = new EventStore[Event]
    val process = new Process[IO] {
      def handle: Receive = {
        case Start => eval(eventStore.add(ref, Start))
        case TestEvent => eval(eventStore.add(ref, TestEvent))
      }
    }

    val sendEvent = TestEvent ~> process
    val processes = Array(process)
    val program = for {
      fiber <- run(processes, sendEvent).start
      _ <- eventStore.awaitSizeOld(expectedEventsCount).guaranteeCase(_ => fiber.cancel)
    } yield ()

    program.unsafeRunSync()

    eventStore.size shouldBe expectedEventsCount
    eventStore.get(process.ref) shouldBe Seq(Start, TestEvent)

  }

  "Stop" should "be delivered last" in {
    val domainEventsCount = 1
    val totalEventsCount = 2 // domainEventsCount + Stop
    val eventStore = new EventStore[Event]
    val process = new Process[IO] {
      def handle: Receive = {
        case TestEvent => eval(eventStore.add(ref, TestEvent))
        case Stop => eval(eventStore.add(ref, Stop))
      }
    }

    val sendEvent = TestEvent ~> process
    val processes = Array(process)
    val program = for {
      fiber <- run(processes, sendEvent).start
      _ <- eventStore.awaitSizeOld(domainEventsCount).guaranteeCase(_ => fiber.cancel)
    } yield ()

    program.unsafeRunSync()

    eventStore.size shouldBe totalEventsCount
    eventStore.get(process.ref) shouldBe Seq(TestEvent, Stop)
  }

  "System shutdown" should "stop child processes first" in {
    val eventStore = new EventStore[Event]
    val trace = ProcessRef("trace")

    val lastProcessCreated = new AtomicBoolean()

    val parent =  new Process[IO] {
      override val ref: ProcessRef = ProcessRef("a")
      override def handle: Receive = {
        case Start => register(ref, new Process[IO] {
          override val ref: ProcessRef = ProcessRef("b")
          override def handle: Receive = {
            case Start => register(ref, new Process[IO] {
              override val ref: ProcessRef = ProcessRef("c")
              override def handle: Receive = {
                case Start => register(ref, new Process[IO] {
                  override val ref: ProcessRef = ProcessRef("d")
                  override def handle: Receive = {
                    case Start => eval(lastProcessCreated.set(true))
                    case Stop => eval(eventStore.add(trace, Stopped(ref.toString))) // ++
                      //reply(sender => eval(println(s"process: $selfRef received Stop from $sender")))
                  }
                })
                case Stop => eval(eventStore.add(trace, Stopped(ref.toString))) // ++
                  //reply(sender => eval(println(s"process: $selfRef received Stop from $sender")))
              }
            })
            case Stop => eval(eventStore.add(trace, Stopped(ref.toString))) // ++
              //reply(sender => eval(println(s"process: $selfRef received Stop from $sender")))
          }
        })
        case Stop => eval(eventStore.add(trace, Stopped(ref.toString))) // ++
          //reply(sender => eval(println(s"process: $selfRef received Stop from $sender")))
      }
    }

    val program = for {
      fiber <- run(Array(parent), unit).start
      _ <- IO.delay(while (!lastProcessCreated.get()) {}) >> fiber.cancel

    } yield ()
    program.unsafeRunSync()


    eventStore.get(trace) shouldBe Seq(Stopped("d"), Stopped("c"), Stopped("b"), Stopped("a"))

  }

  "Kill process" should "immediately terminates process and delivers Stop event" in {
    val eventStore = new EventStore[Event]
    val longRunningProcess: Process[IO] = new Process[IO] {
      override def handle: Receive = {
        case Start => unit
        case Pause => eval(while (true) {})
        case e => eval(eventStore.add(ref, e))
      }
    }

    val flow: DslF[IO, Unit] =
      Seq(Pause, TestEvent, TestEvent, TestEvent) ~> longRunningProcess.ref ++
        delay(1.second, Kill ~> longRunningProcess.ref)

    val program = for {
      fiber <- run(Array(longRunningProcess), flow).start
      _ <- eventStore.awaitSizeOld(1).guaranteeCase(_ => fiber.cancel)
    } yield ()
    program.unsafeRunSync()
    eventStore.print()
    eventStore.size shouldBe 1
    eventStore.get(longRunningProcess.ref) shouldBe Seq(Stop)
  }

  "Stop" should "deliver Stop event and remove process" in {
    val eventStore = new EventStore[Event]
    val process: Process[IO] = new Process[IO] {
      override def handle: Receive = {
        case Start => unit // ignore
        case e => eval(eventStore.add(ref, e))
      }
    }

    val flow: DslF[IO, Unit] = Seq(TestEvent, TestEvent, Stop) ~> process.ref

    val program = for {
      fiber <- run(Array(process), flow).start
      _ <- eventStore.awaitSizeOld(3).guaranteeCase(_ => fiber.cancel)
    } yield ()

    program.unsafeRunSync()
    eventStore.size shouldBe 3
    eventStore.get(process.ref) shouldBe Seq(TestEvent, TestEvent, Stop)

  }

  "An attempt to kill a process more than once" should "return error" in {
    val deadLetterEventStore = new EventStore[DeadLetter]
    val deadLetter = new DeadLetterProcess[IO] {
      def handle: Receive = {
        case f: DeadLetter => eval(deadLetterEventStore.add(ref, f))
      }
    }

    val process: Process[IO] = new Process[IO] {
      override def handle: Receive = {
        case _ => unit
      }
    }

    val flow: DslF[IO, Unit] = delay(1.second, Kill ~> process) ++ Kill ~> process

    val program = for {
      fiber <- run(Array(process), flow, Some(deadLetter)).start
      _ <- deadLetterEventStore.awaitSizeOld(1).guaranteeCase(_ => fiber.cancel)
    } yield ()

    program.unsafeRunSync()

    deadLetterEventStore.get(deadLetter.ref).headOption.value should matchPattern {
      case DeadLetter(Envelope(ProcessRef.SystemRef, Stop, process.`ref`), _) =>
    }

  }

  "An attempt to send Stop event more than once" should "return error" in {
    val deadLetterEventStore = new EventStore[DeadLetter]
    val deadLetter = new DeadLetterProcess[IO] {
      def handle: Receive = {
        case f: DeadLetter => eval(deadLetterEventStore.add(ref, f))
      }
    }

    val process: Process[IO] = new Process[IO] {
      override def handle: Receive = {
        case _ => unit
      }
    }

    val flow: DslF[IO, Unit] = Seq(Stop, Stop) ~> process

    val program = for {
      fiber <- run(Array(process),flow, Some(deadLetter)).start
      _ <- deadLetterEventStore.awaitSizeOld(1).guaranteeCase(_ => fiber.cancel)
    } yield ()

    program.unsafeRunSync()

    deadLetterEventStore.get(deadLetter.ref).headOption.value should matchPattern {
      case DeadLetter(Envelope(ProcessRef.SystemRef, Stop, process.`ref`), _) =>
    }
  }

}

object ProcessLifecycleSpec {

  object TestEvent extends Event

  object Pause extends Event

  case class Stopped(name: String) extends Event

}
