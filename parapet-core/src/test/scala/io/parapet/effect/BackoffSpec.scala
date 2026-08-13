package io.parapet.effect

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import scala.concurrent.duration.*

class BackoffSpec extends AnyFunSuite:

  test("Fixed returns the same delay for every retry") {
    val b = Backoff.Fixed(200.millis)
    b.duration(1).toMillis shouldBe 200
    b.duration(5).toMillis shouldBe 200
  }

  test("Exp grows by factor and caps at max") {
    val b = Backoff.Exp(base = 100.millis, factor = 2.0, max = 1.second)
    b.duration(1).toMillis shouldBe 100 // 100 * 2^0
    b.duration(2).toMillis shouldBe 200 // 100 * 2^1
    b.duration(3).toMillis shouldBe 400
    b.duration(4).toMillis shouldBe 800
    b.duration(5).toMillis shouldBe 1000 // 1600 capped at 1000
    b.duration(10).toMillis shouldBe 1000
  }

  test("Exp rejects a factor below 1") {
    an[IllegalArgumentException] should be thrownBy Backoff.Exp(100.millis, factor = 0.5)
  }
