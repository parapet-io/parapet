package io.parapet.core

import cats.effect.IO
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global

class LockSpec extends FlatSpec {


  implicit val ctx = IO.contextShift(global)
  implicit val timer = IO.timer(global)

  "Lock" should "be released" in {
    val program = for {
      lock <- Lock[IO]
      _ <- lock.withPermit(IO(println("work")))
      acquired <- lock.tryAcquire
    } yield acquired

    program.unsafeRunSync() shouldBe true

  }

  "Failed operation" should "be released" in {
    val program = for {
      lock <- Lock[IO]
      _ <- lock.withPermit(IO.raiseError(new RuntimeException("error!"))).handleErrorWith(_ => IO(println("recover")))
      acquired <- lock.tryAcquire
    } yield acquired

    program.unsafeRunSync() shouldBe true

  }

}
