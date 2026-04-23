package io.parapet.core

import io.parapet.effect.ParIO
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

class LockSpec extends AnyFunSuite:
  private given io.parapet.effect.Effect[ParIO] = ParIO.effect

  test("lock is released after success") {
    val program =
      for
        lock <- Lock[ParIO]()
        _ <- lock.withPermit(ParIO.unit)
        acquired <- lock.tryAcquire
      yield acquired

    program.unsafeRunSync() shouldBe true
  }

  test("lock is released after failure") {
    val program =
      for
        lock <- Lock[ParIO]()
        _ <- lock.withPermit(ParIO.raiseError(new RuntimeException("error"))).handleErrorWith(_ => ParIO.unit)
        acquired <- lock.tryAcquire
      yield acquired

    program.unsafeRunSync() shouldBe true
  }
