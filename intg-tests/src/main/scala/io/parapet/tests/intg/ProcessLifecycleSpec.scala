package io.parapet.tests.intg

import java.util.concurrent.atomic.AtomicBoolean

import cats.syntax.flatMap._
import cats.syntax.functor._
import io.parapet.core.Event._
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.core.{Event, Process, ProcessRef}
import io.parapet.tests.intg.ProcessLifecycleSpec._
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import org.scalatest.OptionValues._

import scala.concurrent.duration._

abstract class ProcessLifecycleSpec[F[_]] extends FlatSpec with IntegrationSpec[F] {

  import dsl._

  "Start event" should "be delivered before client events" in {
    val expectedEventsCount = 2
    val eventStore = new EventStore[F, Event]
    val process = new Process[F] {
      def handle: Receive = {
        case Start => eval(eventStore.add(ref, Start))
        case TestEvent => eval(eventStore.add(ref, TestEvent))
      }
    }

    val init = onStart(TestEvent ~> process)

    unsafeRun(eventStore.await(expectedEventsCount, createApp(ct.pure(Seq(init, process))).run))


    eventStore.size shouldBe expectedEventsCount
    eventStore.get(process.ref) shouldBe Seq(Start, TestEvent)

  }

  "Stop" should "be delivered last" in {
    val domainEventsCount = 1
    val totalEventsCount = 2 // domainEventsCount + Stop
    val eventStore = new EventStore[F, Event]
    val process = new Process[F] {
      def handle: Receive = {
        case TestEvent => eval(eventStore.add(ref, TestEvent))
        case Stop => eval(eventStore.add(ref, Stop))
      }
    }

    val init = onStart(TestEvent ~> process)

    unsafeRun(eventStore.await(domainEventsCount, createApp(ct.pure(Seq(init, process))).run))
    eventStore.size shouldBe totalEventsCount
    eventStore.get(process.ref) shouldBe Seq(TestEvent, Stop)
  }

  "System shutdown" should "stop child processes first" ignore {
    val eventStore = new EventStore[F, Event]
    val trace = ProcessRef("trace")

    val lastProcessCreated = new AtomicBoolean()

    val parent = new Process[F] {
      override val ref: ProcessRef = ProcessRef("a")

      override def handle: Receive = {
        case Start => register(ref, new Process[F] {
          override val ref: ProcessRef = ProcessRef("b")

          override def handle: Receive = {
            case Start => register(ref, new Process[F] {
              override val ref: ProcessRef = ProcessRef("c")

              override def handle: Receive = {
                case Start => register(ref, new Process[F] {
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
      fiber <- ct.start(createApp(ct.pure(Seq(parent))).run)
      _ <- ct.delay(while (!lastProcessCreated.get()) {}) >> fiber.cancel

    } yield ()
    unsafeRun(program)

    eventStore.get(trace) shouldBe Seq(Stopped("d"), Stopped("c"), Stopped("b"), Stopped("a"))

  }

  "Send an event to the registered precess within the same flow" should "deliver" in {
    val eventStore = new EventStore[F, Event]

    val child = ProcessRef("child")
    val process: Process[F] = Process[F](ref => {
      case Start => evalWith(Process.builder[F](_ => {
        case TestEvent => eval(eventStore.add(child, TestEvent))
      }).ref(child).build) { p => {
        register(ref, p) ++ TestEvent ~> p
      }
      }
    })
    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(process))).run))

    eventStore.size shouldBe 1
    eventStore.get(child) shouldBe Seq(TestEvent)

  }

  "Kill process" should "immediately terminates process and delivers Stop event" ignore {
    val eventStore = new EventStore[F, Event]
    val longRunningProcess: Process[F] = new Process[F] {
      override def handle: Receive = {
        case Start => unit
        case Pause => delay(1.minute)
        case e => eval(eventStore.add(ref, e))
      }
    }

    val init = onStart(Seq(Pause, TestEvent, TestEvent, TestEvent) ~> longRunningProcess.ref ++
      delay(1.second, Kill ~> longRunningProcess.ref))


    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(init, longRunningProcess))).run))

    eventStore.size shouldBe 1
    eventStore.get(longRunningProcess.ref) shouldBe Seq(Stop)
  }

  "Stop" should "deliver Stop event and remove process" in {
    val eventStore = new EventStore[F, Event]
    val process: Process[F] = new Process[F] {
      override def handle: Receive = {
        case Start => unit // ignore
        case e => eval(eventStore.add(ref, e))
      }
    }

    val init = onStart(Seq(TestEvent, TestEvent, Stop) ~> process.ref)

    unsafeRun(eventStore.await(3, createApp(ct.pure(Seq(init, process))).run))


    eventStore.size shouldBe 3
    eventStore.get(process.ref) shouldBe Seq(TestEvent, TestEvent, Stop)

  }

  "An attempt to kill a process more than once" should "return error" ignore {
    val eventStore = new EventStore[F, DeadLetter]
    val deadLetter = new DeadLetterProcess[F] {
      def handle: Receive = {
        case DeadLetter(Envelope(_, Start, _), _) => unit // Start event can be interrupted, ignore
        case DeadLetter(Envelope(_, Kill, _), _) => unit // in case if the process was removed
        // from the system before second Kill event added to it's queue
        case f@DeadLetter(Envelope(_, Stop, _), _) => eval(eventStore.add(ref, f))
      }
    }

    val process: Process[F] = new Process[F] {
      override def handle: Receive = {
        case _ => unit
      }
    }

    val init = onStart(Seq(Kill, Kill) ~> process)
    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(init, process)), Some(ct.pure(deadLetter))).run))

    eventStore.get(deadLetter.ref).headOption.value should matchPattern {
      case DeadLetter(Envelope(TestSystemRef, Stop, process.ref), _) =>
    }

  }

  "An attempt to send Stop event more than once" should "return error" ignore {
    val eventStore = new EventStore[F, DeadLetter]
    val deadLetter = new DeadLetterProcess[F] {
      def handle: Receive = {
        case f: DeadLetter => eval(eventStore.add(ref, f))
      }
    }

    val process: Process[F] = new Process[F] {
      override def handle: Receive = {
        case _ => unit
      }
    }

    val init = onStart(Seq(Stop, Stop) ~> process)

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(init, process)), Some(ct.pure(deadLetter))).run))

    eventStore.get(deadLetter.ref).headOption.value should matchPattern {
      case DeadLetter(Envelope(TestSystemRef, Stop, process.`ref`), _) =>
    }
  }

}

object ProcessLifecycleSpec {

  object TestEvent extends Event

  object Pause extends Event

  case class Stopped(name: String) extends Event

}