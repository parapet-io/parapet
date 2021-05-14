package io.parapet.core

import cats.effect.{ContextShift, IO, Timer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global

class LockSpec extends AnyFlatSpec {


  private implicit val ctx: ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO] = IO.timer(global)

  "Lock" should "be released" in {
    val program = for {
      lock <- Lock[IO]
      _ <- lock.withPermit(IO.unit)
      acquired <- lock.tryAcquire
    } yield acquired

    program.unsafeRunSync() shouldBe true

  }

  "Failed operation" should "be released" in {
    val program = for {
      lock <- Lock[IO]
      _ <- lock.withPermit(IO.raiseError(new RuntimeException("error"))).handleErrorWith(_ => IO.unit)
      acquired <- lock.tryAcquire
    } yield acquired

    program.unsafeRunSync() shouldBe true

  }

}
