package io.parapet.core

import io.parapet.core.Dsl.WithDsl
import io.parapet.syntax.FlowSyntax
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

class FlowSyntaxSpec extends AnyFunSuite with WithDsl[TestUtils.TestIO] with FlowSyntax[TestUtils.TestIO]:
  import TestUtils.*
  import TestUtils.given
  import dsl.*

  test("guarantee runs finalizer on failure") {
    var fallbackCalled = false

    def fallback: Dsl.DslF[TestIO, Unit] =
      eval {
        fallbackCalled = true
      }.void

    val fixture = new RuntimeFixture

    assertThrows[RuntimeException] {
      fixture.run(eval(throw new RuntimeException("error")).guarantee(fallback))
    }

    fallbackCalled shouldBe true
  }
