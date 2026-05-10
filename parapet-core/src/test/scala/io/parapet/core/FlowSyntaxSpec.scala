package io.parapet.core

import io.parapet.core.Dsl.WithDsl
import io.parapet.syntax.FlowSyntax
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

class FlowSyntaxSpec extends AnyFunSuite with WithDsl[TestUtils.Id] with FlowSyntax[TestUtils.Id]:
  import TestUtils.*
  import TestUtils.given
  import dsl.*

  test("guarantee runs finalizer on failure") {
    var fallbackCalled = false

    def fallback: Dsl.DslF[Id, Unit] =
      eval {
        fallbackCalled = true
      }.void

    val interpreter = new IdInterpreter()

    assertThrows[RuntimeException] {
      eval(throw new RuntimeException("error")).guarantee(fallback).foldMap(interpreter)
    }

    fallbackCalled shouldBe true
  }
