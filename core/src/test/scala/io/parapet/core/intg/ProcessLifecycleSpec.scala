package io.parapet.core.intg

import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.IO
import io.parapet.core.Dsl.{DslF, WithDsl}
import io.parapet.core.Event._
import io.parapet.core.intg.ProcessLifecycleSpec._
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.core.testutils.{EventStore, IntegrationSpec}
import io.parapet.core.{Event, Process, ProcessRef}
import io.parapet.implicits._
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import org.scalatest.OptionValues._
import cats.syntax.flatMap._

import scala.concurrent.duration._

class ProcessLifecycleSpec extends FlatSpec with IntegrationSpec with WithDsl[IO] {

  import effectDsl._
  import flowDsl._

  "Start event" should "be delivered before client events" in {
    val expectedEventsCount = 2
    val eventStore = new EventStore[Event]
    val process = new Process[IO] {
      def handle: Receive = {
        case Start => eval(eventStore.add(selfRef, Start))
        case TestEvent => eval(eventStore.add(selfRef, TestEvent))
      }
    }

    val sendEvent = TestEvent ~> process
    val processes = Array(process)
    val program = for {
      fiber <- run(sendEvent, processes).start
      _ <- eventStore.awaitSize(expectedEventsCount).guaranteeCase(_ => fiber.cancel)
    } yield ()

    program.unsafeRunSync()

    eventStore.size shouldBe expectedEventsCount
    eventStore.get(process.selfRef) shouldBe Seq(Start, TestEvent)

  }

  "Stop" should "be delivered last" in {
    val domainEventsCount = 1
    val totalEventsCount = 2 // domainEventsCount + Stop
    val eventStore = new EventStore[Event]
    val process = new Process[IO] {
      def handle: Receive = {
        case TestEvent => eval(eventStore.add(selfRef, TestEvent))
        case Stop => eval(eventStore.add(selfRef, Stop))
      }
    }

    val sendEvent = TestEvent ~> process
    val processes = Array(process)
    val program = for {
      fiber <- run(sendEvent, processes).start
      _ <- eventStore.awaitSize(domainEventsCount).guaranteeCase(_ => fiber.cancel)
    } yield ()

    program.unsafeRunSync()

    eventStore.size shouldBe totalEventsCount
    eventStore.get(process.selfRef) shouldBe Seq(TestEvent, Stop)
  }

  "System shutdown" should "stop child processes first" in {
    val eventStore = new EventStore[Event]
    val trace = ProcessRef("trace")

    val lastProcessCreated = new AtomicBoolean()

    val parent =  new Process[IO] {
      override val selfRef: ProcessRef = ProcessRef("a")
      override def handle: Receive = {
        case Start => register(selfRef, new Process[IO] {
          override val selfRef: ProcessRef = ProcessRef("b")
          override def handle: Receive = {
            case Start => register(selfRef, new Process[IO] {
              override val selfRef: ProcessRef = ProcessRef("c")
              override def handle: Receive = {
                case Start => register(selfRef, new Process[IO] {
                  override val selfRef: ProcessRef = ProcessRef("d")
                  override def handle: Receive = {
                    case Start => eval(lastProcessCreated.set(true))
                    case Stop => eval(eventStore.add(trace, Stopped(selfRef.toString))) // ++
                      //reply(sender => eval(println(s"process: $selfRef received Stop from $sender")))
                  }
                })
                case Stop => eval(eventStore.add(trace, Stopped(selfRef.toString))) // ++
                  //reply(sender => eval(println(s"process: $selfRef received Stop from $sender")))
              }
            })
            case Stop => eval(eventStore.add(trace, Stopped(selfRef.toString))) // ++
              //reply(sender => eval(println(s"process: $selfRef received Stop from $sender")))
          }
        })
        case Stop => eval(eventStore.add(trace, Stopped(selfRef.toString))) // ++
          //reply(sender => eval(println(s"process: $selfRef received Stop from $sender")))
      }
    }

    val program = for {
      fiber <- run(empty, Array(parent)).start
      _ <- IO.delay(while (!lastProcessCreated.get()) {}) >> fiber.cancel

    } yield ()
    program.unsafeRunSync()


    eventStore.get(trace) shouldBe Seq(Stopped("d"), Stopped("c"), Stopped("b"), Stopped("a"))

  }

  "Kill process" should "immediately terminates process and delivers Stop event" in {
    val eventStore = new EventStore[Event]
    val longRunningProcess: Process[IO] = new Process[IO] {
      override def handle: Receive = {
        case Start => empty
        case Pause => eval(while (true) {})
        case e => eval(eventStore.add(selfRef, e))
      }
    }

    val flow: DslF[IO, Unit] =
      Seq(Pause, TestEvent, TestEvent, TestEvent) ~> longRunningProcess.selfRef ++
        delay(1.second, Kill ~> longRunningProcess.selfRef)

    val program = for {
      fiber <- run(flow, Array(longRunningProcess)).start
      _ <- eventStore.awaitSize(1).guaranteeCase(_ => fiber.cancel)
    } yield ()
    program.unsafeRunSync()
    eventStore.print()
    eventStore.size shouldBe 1
    eventStore.get(longRunningProcess.selfRef) shouldBe Seq(Stop)
  }

  "Stop" should "deliver Stop event and remove process" in {
    val eventStore = new EventStore[Event]
    val process: Process[IO] = new Process[IO] {
      override def handle: Receive = {
        case Start => empty // ignore
        case e => eval(eventStore.add(selfRef, e))
      }
    }

    val flow: DslF[IO, Unit] = Seq(TestEvent, TestEvent, Stop) ~> process.selfRef

    val program = for {
      fiber <- run(flow, Array(process)).start
      _ <- eventStore.awaitSize(3).guaranteeCase(_ => fiber.cancel)
    } yield ()

    program.unsafeRunSync()
    eventStore.size shouldBe 3
    eventStore.get(process.selfRef) shouldBe Seq(TestEvent, TestEvent, Stop)

  }

  "An attempt to kill a process more than once" should "return error" in {
    val deadLetterEventStore = new EventStore[DeadLetter]
    val deadLetter = new DeadLetterProcess[IO] {
      def handle: Receive = {
        case f: DeadLetter => eval(deadLetterEventStore.add(selfRef, f))
      }
    }

    val process: Process[IO] = new Process[IO] {
      override def handle: Receive = {
        case _ => empty
      }
    }

    val flow: DslF[IO, Unit] = delay(1.second, Kill ~> process) ++ Kill ~> process

    val program = for {
      fiber <- run(flow, Array(process), Some(deadLetter)).start
      _ <- deadLetterEventStore.awaitSize(1).guaranteeCase(_ => fiber.cancel)
    } yield ()

    program.unsafeRunSync()

    deadLetterEventStore.get(deadLetter.selfRef).headOption.value should matchPattern {
      case DeadLetter(Envelope(ProcessRef.SystemRef, Stop, process.selfRef), _) =>
    }

  }

  "An attempt to send Stop event more than once" should "return error" in {
    val deadLetterEventStore = new EventStore[DeadLetter]
    val deadLetter = new DeadLetterProcess[IO] {
      def handle: Receive = {
        case f: DeadLetter => eval(deadLetterEventStore.add(selfRef, f))
      }
    }

    val process: Process[IO] = new Process[IO] {
      override def handle: Receive = {
        case _ => empty
      }
    }

    val flow: DslF[IO, Unit] = Seq(Stop, Stop) ~> process

    val program = for {
      fiber <- run(flow, Array(process), Some(deadLetter)).start
      _ <- deadLetterEventStore.awaitSize(1).guaranteeCase(_ => fiber.cancel)
    } yield ()

    program.unsafeRunSync()

    deadLetterEventStore.get(deadLetter.selfRef).headOption.value should matchPattern {
      case DeadLetter(Envelope(ProcessRef.SystemRef, Stop, process.selfRef), _) =>
    }
  }

}

object ProcessLifecycleSpec {

  object TestEvent extends Event

  object Pause extends Event

  case class Stopped(name: String) extends Event

}
