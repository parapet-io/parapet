package io.parapet.tests.intg

import io.parapet.core.Events.{Failure, Kill, Start, Stop}
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Process
import io.parapet.core.Scheduler.SchedulerConfig
import io.parapet.core.exceptions.EventHandlingException
import io.parapet.testutils.EventStore
import io.parapet.{Envelope, Event, ProcessRef}
import org.scalatest.OptionValues._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.util.Random

abstract class BlockingSpec[F[_]] extends AnyFunSuite with IntegrationSpec[F] {

  import BlockingSpec._
  import dsl._

  test("blocking") {

    val eventStore = new EventStore[F, TestEvent.type]

    val slowRef = ProcessRef("slow")
    val fastRef = ProcessRef("fast")

    val slowProcess = Process
      .builder[F](_ => { case Start =>
        TestEvent ~> fastRef ++ offload(delay(1.hour)) ++ TestEvent ~> fastRef
      })
      .ref(slowRef)
      .name("slow")
      .build

    val fastProcess = Process
      .builder[F](ref => { case TestEvent =>
        eval(eventStore.add(ref, TestEvent))
      })
      .ref(fastRef)
      .name("fast")
      .build

    unsafeRun(
      eventStore.await(
        2,
        createApp(
          ct.pure(Seq(slowProcess, fastProcess)),
          config0 = ParConfig(-1, SchedulerConfig(numberOfWorkers = 1))
        ).run
      )
    )
    eventStore.get(fastProcess.ref).headOption.value shouldBe TestEvent
  }

  test("multiple blocking") {
    val eventStore = new EventStore[F, Event]
    val total      = 10
    val count      = new AtomicInteger()

    val process = Process(ref => {
      case Start =>
        (0 until total)
          .map(_ => offload(delay((1 + Random.nextInt(3)).seconds) ++ eval(count.incrementAndGet())))
          .reduce(_ ++ _) ++ Done ~> ref

      case Done => eval(eventStore.add(ref, NumEvent(count.get())))
    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(process))).run))
    eventStore.get(process.ref) shouldBe Seq(NumEvent(10))

  }

  test("stop blocking operation") {
    val eventStore = new EventStore[F, Event]

    val process = Process(ref => {
      case Start =>
        offload {
          eval(println("start blocking")) ++
            delay(1.minute)
        }
      case Stop => eval(eventStore.add(ref, Stop))
    })

    unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(onStart(Kill ~> process), process))).run))
    eventStore.get(process.ref) shouldBe Seq(Stop)
  }

  test("blocking failure is reported and does not wedge the process") {
    val eventStore = new EventStore[F, Event]

    case object ServerError extends RuntimeException("error")

    val serverRef = ProcessRef("blocking-server")
    val clientRef = ProcessRef("blocking-client")

    val server = Process
      .builder[F](ref => {
        case Trigger =>
          offload(raiseError(ServerError))
        case Ping =>
          eval(eventStore.add(ref, Ping))
      })
      .ref(serverRef)
      .build

    val client = Process
      .builder[F](ref => {
        case Start =>
          Trigger ~> serverRef ++
            delay(50.millis) ++
            Ping ~> serverRef
        case failure: Failure =>
          eval(eventStore.add(ref, failure))
      })
      .ref(clientRef)
      .build

    unsafeRun(eventStore.await(2, createApp(ct.pure(Seq(client, server))).run))

    eventStore.get(server.ref) shouldBe Seq(Ping)
    eventStore.get(client.ref).headOption.value should matchPattern {
      case Failure(
            Envelope(client.`ref`, Trigger, server.`ref`),
            EventHandlingException(_, ServerError)
          ) =>
    }
  }

}

object BlockingSpec {

  object TestEvent extends Event

  object Done extends Event

  case class NumEvent(i: Int) extends Event

  object Trigger extends Event

  object Ping extends Event

}
