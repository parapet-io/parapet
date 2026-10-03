package io.parapet.cats

import cats.effect.{Deferred, IO, Outcome as CatsOutcome, Ref}
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import io.parapet.effect.{Effect, Outcome as FiberOutcome}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.util.concurrent.CancellationException

class CatsEffectParapetRuntimeSpec extends AnyFunSuite:
  private def observeOutcome[A](runtime: CatsEffectParapetRuntime)(fa: IO[A]): IO[FiberOutcome[A]] =
    Effect.observeOutcome(fa)(using runtime.effect)

  test("observing a child fiber materializes success, failure, and self-cancellation") {
    val runtime           = CatsEffectParapetRuntime()
    val error             = new RuntimeException("boom")
    val cancellationError = new CancellationException("ordinary failure")

    try
      observeOutcome(runtime)(IO.pure(42)).unsafeRunSync() shouldBe FiberOutcome.Succeeded(42)
      observeOutcome(runtime)(IO.raiseError[Int](error)).unsafeRunSync() shouldBe FiberOutcome.Failed(error)
      observeOutcome(runtime)(IO.raiseError[Int](cancellationError)).unsafeRunSync() shouldBe FiberOutcome.Failed(
        cancellationError
      )
      observeOutcome(runtime)(IO.canceled).unsafeRunSync() shouldBe FiberOutcome.Canceled()
    finally runtime.close()
  }

  test("canceling a child-fiber observer cancels the child and remains cancellation") {
    val runtime = CatsEffectParapetRuntime()
    val program =
      for
        started   <- Deferred[IO, Unit]
        finalized <- Ref.of[IO, Boolean](false)
        child = (started.complete(()) >> IO.never).onCancel(finalized.set(true))
        observer  <- observeOutcome(runtime)(child).start
        _         <- started.get
        _         <- observer.cancel
        result    <- observer.join
        cleanedUp <- finalized.get
      yield (result, cleanedUp)

    try
      val (result, cleanedUp) = program.unsafeRunSync()
      result shouldBe CatsOutcome.Canceled()
      cleanedUp shouldBe true
    finally runtime.close()
  }
