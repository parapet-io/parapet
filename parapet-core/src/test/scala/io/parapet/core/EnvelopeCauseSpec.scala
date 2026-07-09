package io.parapet.core

import io.parapet.core.Dsl.WithDsl
import io.parapet.core.EnvelopeCauseSpec.*
import io.parapet.core.TestUtils.*
import io.parapet.core.TestUtils.given
import io.parapet.syntax.FlowSyntax
import io.parapet.{Envelope, Event, ProcessRef, Scope}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

class EnvelopeCauseSpec extends AnyFunSuite with WithDsl[TestIO] with FlowSyntax[TestIO]:

  import dsl.*

  test("a root envelope (no Scope.Cause) reports cause 0") {
    Envelope(ProcessRef.SystemRef, Ping, target).cause shouldBe 0L
  }

  test("causalScope stamps the envelope's own id under Scope.Cause") {
    val parent = Envelope(ProcessRef.SystemRef, Ping, target)

    parent.causalScope.get(Scope.Cause) shouldBe Some(parent.id)

    // an envelope built under parent.causalScope is causally linked back to parent
    val child = Envelope(ProcessRef.SystemRef, Ping, target, parent.causalScope)
    child.cause shouldBe parent.id
    child.id should not be parent.id
  }

  test("an emitted envelope inherits cause from the interpreting scope") {
    val parent  = Envelope(ProcessRef.SystemRef, Ping, target)
    val fixture = new RuntimeFixture

    // interpret a handler exactly as the runtime's delivery seam does: under parent.causalScope
    fixture.runWithSender(
      sender = ProcessRef.SystemRef,
      program = Ping ~> target,
      scope = parent.causalScope
    )

    fixture.captured should have size 1
    val emitted = fixture.captured.head
    emitted.cause shouldBe parent.id
    emitted.id should not be parent.id
  }

object EnvelopeCauseSpec:
  private case object Ping extends Event
  private val target = ProcessRef[Event]("target")
