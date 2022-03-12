package io.parapet.core

import io.parapet.core.Dsl.WithDsl
import io.parapet.instances.interpreter.EvalInterpreter
import io.parapet.syntax.FlowSyntax
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class FlowSyntaxSpec extends AnyFunSuite with WithDsl[cats.Eval] with FlowSyntax[cats.Eval] {

  import dsl._

  test("guaranteed") {
    var fallbackCalled = false

    def fallback = eval {
      fallbackCalled = true
    }

    assertThrows[RuntimeException] {
      eval(throw new RuntimeException("error")).guarantee(fallback).foldMap(new EvalInterpreter())
    }

    fallbackCalled shouldBe true

  }

  test("handleError") {

  }

}
