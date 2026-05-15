package io.parapet.core

import io.parapet.{Event, ProcessRef}
import io.parapet.core.Dsl.WithDsl
import io.parapet.core.Events.Start
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

class ProcessSpec extends AnyFunSuite with WithDsl[TestUtils.Id]:
  import TestUtils.*
  import TestUtils.given
  import dsl.*

  sealed trait Command extends Event
  case object Ping     extends Command
  case object Request  extends Event
  case object Response extends Event

  test("and composes system event handlers for typed processes") {
    var seen = Vector.empty[String]

    val left = Process.typed[Id, Command](_ => {
      case Start => eval { seen = seen :+ "left" }
      case Ping  => unit
    })

    val right = Process.typed[Id, Command](_ => {
      case Start => eval { seen = seen :+ "right" }
      case Ping  => unit
    })

    val composed = left.and(right)

    composed.canHandle(Start) shouldBe true
    composed(Start).foldMap(new IdInterpreter())
    seen shouldBe Vector("left", "right")
  }

  test("or dispatches system events to the branch that handles them") {
    var seen = Vector.empty[String]

    val domainOnly = Process.typed[Id, Command](_ => { case Ping => unit })

    val lifecycleAware = Process.typed[Id, Command](_ => {
      case Start => eval { seen = seen :+ "lifecycle" }
      case Ping  => unit
    })

    val composed = domainOnly.or(lifecycleAware)

    composed.canHandle(Start) shouldBe true
    composed(Start).foldMap(new IdInterpreter())
    seen shouldBe Vector("lifecycle")
  }

  test("reply is checked against the process output protocol") {
    val clientRef = ProcessRef[Response.type]("client")
    val execution = new Execution()

    val server = new Process[Id, Request.type, Response.type]:
      import dsl.*

      override def handle: Receive = { case Request =>
        reply(Response)
      }

    server(Request).foldMap(new IdInterpreter(execution, senderRef = clientRef))

    execution.trace.toSeq shouldBe Seq(Message(Response, clientRef))
  }
