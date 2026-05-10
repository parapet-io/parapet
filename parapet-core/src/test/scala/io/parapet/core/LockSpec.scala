package io.parapet.core

import io.parapet.core.TestUtils.TestIO
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

class LockSpec extends AnyFunSuite:
  import TestUtils.given

  test("lock is released after success") {
    val program =
      for
        lock     <- Lock[TestIO]()
        _        <- lock.withPermit(TestIO.unit)
        acquired <- lock.tryAcquire
      yield acquired

    program.unsafeRun() shouldBe true
  }

  test("lock is released after failure") {
    val program =
      for
        lock     <- Lock[TestIO]()
        _        <- lock.withPermit(TestIO.raiseError(new RuntimeException("error"))).handleErrorWith(_ => TestIO.unit)
        acquired <- lock.tryAcquire
      yield acquired

    program.unsafeRun() shouldBe true
  }
