package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Dsl.WithDsl
import io.parapet.core.Event.Start
import io.parapet.core.intg.AwaitEventSpec._
import io.parapet.core.testutils.{EventStore, IntegrationSpec}
import io.parapet.core.{Event, Process, ProcessRef}
import io.parapet.syntax.event._
import io.parapet.syntax.flow._
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.OptionValues._

import scala.concurrent.duration._

class AwaitEventSpec extends FunSuite  with IntegrationSpec with WithDsl[IO] {

  import effectDsl._
  import flowDsl._

  test("event delivered") {

    val eventStore = new EventStore[Event]

    val server = new Process[IO] {

      override val selfRef: ProcessRef = ProcessRef("server")

      override def handle: Receive = {
        case Request => reply(sender => Response ~> sender)
      }
    }

    val client = new Process[IO] {
      override def handle: Receive = {
        case Start => Request ~> server.selfRef ++
          await { case Response => }(Timeout ~> selfRef, 100.millis)
        case r@Response => eval(eventStore.add(selfRef, r))
      }
    }

    val processes = Array(client, server)

    val program = for {
      fiber <- run(empty, processes).start
      _ <- eventStore.awaitSize(1).guaranteeCase(_ => fiber.cancel)

    } yield ()
    program.unsafeRunSync()

    eventStore.size shouldBe 1
    eventStore.get(client.selfRef).headOption.value should matchPattern {
      case Response =>
    }


  }
  test("await timedout") {

    val eventStore = new EventStore[Event]
    val serverRef = ProcessRef("server")

    val server = new Process[IO] {

      override val selfRef: ProcessRef = serverRef

      override def handle: Receive = {
        case Request => delay(200.millis) ++ reply(sender => Response ~> sender)
      }
    }

    val client = new Process[IO] {
      override def handle: Receive = {
        case Start =>
          Request ~> serverRef ++
            await { case Response => }(Timeout ~> selfRef, 100.millis)
        case r@Response => eval(eventStore.add(selfRef, r))
        case t@Timeout => eval(eventStore.add(selfRef, t))
      }
    }

    val processes = Array(client, server)

    val program = for {
      fiber <- run(empty, processes).start
      _ <- eventStore.awaitSize(1).guaranteeCase(_ => fiber.cancel)

    } yield ()
    program.unsafeRunSync()

    eventStore.size shouldBe 1
    eventStore.get(client.selfRef).headOption.value should matchPattern {
      case Timeout =>
    }

  }

}


object AwaitEventSpec {

  object Request extends Event

  object Response extends Event

  object Timeout extends Event

}