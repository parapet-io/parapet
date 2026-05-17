package io.parapet.core

import io.parapet.core.Dsl.{DslF, WithDsl}
import io.parapet.core.ScopeSpec.*
import io.parapet.core.TestUtils.*
import io.parapet.core.TestUtils.given
import io.parapet.syntax.FlowSyntax
import io.parapet.{Event, ProcessRef, Scope}
import org.scalatest.OptionValues.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.util.concurrent.atomic.AtomicReference

class ScopeSpec extends AnyFunSuite with WithDsl[TestIO] with FlowSyntax[TestIO]:

  import dsl.*

  private val targetRef = ProcessRef[Event]("target")

  test("withScope sees the initial reader-context scope") {
    val seen    = new AtomicReference[Scope](Scope.empty)
    val fixture = new RuntimeFixture

    val program: DslF[TestIO, Unit] =
      withScope(s => eval(seen.set(s)))

    fixture.run(program, Scope.empty.put(Tag, "hello"))

    seen.get().get(Tag) shouldBe Some("hello")
  }

  test("mapScope only applies to its body; outer scope is restored for subsequent sends") {
    val fixture = new RuntimeFixture

    val program: DslF[TestIO, Unit] =
      mapScope(_.put(Tag, "inside")) {
        Ping ~> targetRef
      } ++ Pong ~> targetRef

    fixture.run(program)

    val byEvent = fixture.captured.map(env => env.event -> env.scope.get(Tag)).toList
    byEvent shouldBe List(
      Ping -> Some("inside"),
      Pong -> None
    )
  }

  test("an outbound ~> stamps the current scope onto the envelope") {
    val fixture = new RuntimeFixture

    val program: DslF[TestIO, Unit] = Ping ~> targetRef

    fixture.run(program, Scope.empty.put(Tag, "from-inbound"))

    fixture.captured should have size 1
    fixture.captured.head.scope.get(Tag).value shouldBe "from-inbound"
  }

  test("reply carries the current scope back to the original sender") {
    val fixture = new RuntimeFixture

    val program: DslF[TestIO, Unit] = reply(Pong)

    fixture.runWithSender(
      sender = targetRef,
      program = program,
      scope = Scope.empty.put(Tag, "round-trip")
    )

    fixture.captured should have size 1
    val replyEnv = fixture.captured.head
    replyEnv.event shouldBe Pong
    replyEnv.receiver shouldBe targetRef
    replyEnv.scope.get(Tag).value shouldBe "round-trip"
  }

  test("forward stamps the current scope and preserves the original sender") {
    val fixture = new RuntimeFixture

    val program: DslF[TestIO, Unit] = forward(Ping, targetRef)

    fixture.runWithSender(
      sender = ProcessRef[Event]("origin"),
      program = program,
      scope = Scope.empty.put(Tag, "via-proxy")
    )

    fixture.captured should have size 1
    val env = fixture.captured.head
    env.event shouldBe Ping
    env.receiver shouldBe targetRef
    env.sender shouldBe ProcessRef[Event]("origin")
    env.scope.get(Tag).value shouldBe "via-proxy"
  }

object ScopeSpec:

  private case object Ping extends Event

  private case object Pong extends Event

  private case object Tag extends Scope.Key[String]:
    val name: String = "io.parapet.test.tag"
