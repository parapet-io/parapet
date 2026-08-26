package io.parapet.effect

import io.parapet.TestUtils.{*, given}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import scala.concurrent.duration.*

class RetrySpec extends AnyFunSuite:

  private val noWait = Backoff.Fixed(Duration.Zero)

  test("returns immediately on success, no retries") {
    var attempts = 0
    val r        = Retry[TestIO, Int](maxRetries = 3, noWait)(TestIO.delay { attempts += 1; 42 })
    r.unsafeRun() shouldBe 42
    attempts shouldBe 1
  }

  test("retries until success within maxRetries") {
    var attempts = 0
    val r        = Retry[TestIO, Int](maxRetries = 5, noWait) {
      TestIO.delay {
        attempts += 1
        if attempts < 3 then throw new RuntimeException("boom") else 7
      }
    }
    r.unsafeRun() shouldBe 7
    attempts shouldBe 3 // fail, fail, succeed
  }

  test("propagates the last error after maxRetries are exhausted") {
    var attempts = 0
    val r        = Retry[TestIO, Int](maxRetries = 2, noWait) {
      TestIO.delay { attempts += 1; throw new RuntimeException(s"boom-$attempts") }
    }
    val error = the[RuntimeException] thrownBy r.unsafeRun()
    error.getMessage shouldBe "boom-3" // initial attempt + 2 retries
    attempts shouldBe 3
  }

  test("maxRetries = 0 runs exactly once, then propagates") {
    var attempts = 0
    val r        = Retry[TestIO, Int](maxRetries = 0, noWait)(TestIO.delay {
      attempts += 1; throw new RuntimeException("once")
    })
    an[RuntimeException] should be thrownBy r.unsafeRun()
    attempts shouldBe 1
  }

  test("invokes onRetry with the retry number before each retry") {
    var seen = List.empty[Int]
    val r    = Retry[TestIO, Int](maxRetries = 3, noWait, (n, _) => seen = seen :+ n) {
      TestIO.delay(throw new RuntimeException("x"))
    }
    an[RuntimeException] should be thrownBy r.unsafeRun()
    seen shouldBe List(1, 2, 3)
  }
