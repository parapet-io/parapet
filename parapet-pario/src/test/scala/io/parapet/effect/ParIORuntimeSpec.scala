package io.parapet.effect

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.util.concurrent.{Callable, CancellationException, CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.*

class ParIORuntimeSpec extends AnyFunSuite:
  private def observeOutcome[A](runtime: ParIORuntime)(fa: ParIO[A]): ParIO[Outcome[A]] =
    Effect.observeOutcome(fa)(using runtime.effect)

  private def succeededValue[A](outcome: Outcome[A]): A =
    outcome match
      case Outcome.Succeeded(value) => value
      case Outcome.Failed(error)    => throw error
      case Outcome.Canceled()       => fail("expected the fiber to succeed, but it was canceled")

  private def testRuntime(asyncSize: Int = 2): ParIORuntime =
    new ParIORuntime(
      ParIORuntimeConfig(
        scheduler = ElasticPoolConfig(
          coreSize = 2,
          maxSize = Int.MaxValue,
          keepAlive = 30.seconds,
          threadNamePrefix = "test-scheduler"
        ),
        parallel = FixedPoolConfig(2, "test-parallel"),
        async = FixedPoolConfig(asyncSize, "test-async"),
        observer = ElasticPoolConfig(
          coreSize = 0,
          maxSize = 4,
          keepAlive = 30.seconds,
          threadNamePrefix = "test-observer"
        ),
        blocking = ElasticPoolConfig(
          coreSize = 0,
          maxSize = 4,
          keepAlive = 30.seconds,
          threadNamePrefix = "test-blocking"
        ),
        race = ElasticPoolConfig(
          coreSize = 0,
          maxSize = 8,
          keepAlive = 30.seconds,
          threadNamePrefix = "test-race"
        ),
        timer = TimerThreadPoolConfig(1, "test-timer")
      )
    )

  test("blocking shifts work onto the blocking pool") {
    val runtime = testRuntime()
    try
      val threadName = runtime.unsafeRun(ParIO.blocking(Thread.currentThread().getName))
      threadName should startWith("test-blocking-")
    finally runtime.shutdown()
  }

  test("startBlocking runs the fiber on the blocking pool") {
    val runtime = testRuntime()
    try
      val fiber = runtime.unsafeRun(runtime.effect.startBlocking(ParIO.delay(Thread.currentThread().getName)))
      succeededValue(runtime.unsafeRun(fiber.join)) should startWith("test-blocking-")
    finally runtime.shutdown()
  }

  test("join materializes a fiber failure") {
    val runtime = testRuntime()
    val error   = new RuntimeException("boom")

    try
      val fiber = runtime.unsafeRun(runtime.effect.start(ParIO.raiseError[Int](error)))
      runtime.unsafeRun(fiber.join) shouldBe Outcome.Failed(error)
    finally runtime.shutdown()
  }

  test("observing a child fiber materializes success, failure, and self-cancellation") {
    val runtime           = testRuntime()
    val error             = new RuntimeException("boom")
    val cancellationError = new CancellationException("ordinary failure")

    try
      runtime.unsafeRun(observeOutcome(runtime)(ParIO.pure(42))) shouldBe Outcome.Succeeded(42)
      runtime.unsafeRun(observeOutcome(runtime)(ParIO.raiseError[Int](error))) shouldBe Outcome.Failed(error)
      runtime.unsafeRun(observeOutcome(runtime)(ParIO.raiseError[Int](cancellationError))) shouldBe Outcome.Failed(
        cancellationError
      )
      runtime.unsafeRun(observeOutcome(runtime)(runtime.effect.canceled)) shouldBe Outcome.Canceled()
    finally runtime.shutdown()
  }

  test("observing a child makes progress when the bounded async pool is occupied") {
    val runtime          = testRuntime(asyncSize = 1)
    val occupied         = new CountDownLatch(1)
    val release          = new CountDownLatch(1)
    val observerExecutor = Executors.newSingleThreadExecutor()

    val longLived = runtime.unsafeRun(
      runtime.effect.start(
        ParIO.delay {
          occupied.countDown()
          release.await()
        }
      )
    )

    try
      occupied.await(1, TimeUnit.SECONDS) shouldBe true

      val observed = observerExecutor.submit(new Callable[Outcome[Int]]() {
        override def call(): Outcome[Int] =
          runtime.unsafeRun(observeOutcome(runtime)(ParIO.pure(42)))
      })
      observed.get(1, TimeUnit.SECONDS) shouldBe Outcome.Succeeded(42)
    finally
      release.countDown()
      runtime.unsafeRun(longLived.join)
      observerExecutor.shutdownNow()
      runtime.shutdown()
  }

  test("canceling a child-fiber observer cancels the child and remains cancellation") {
    val runtime   = testRuntime()
    val started   = new CountDownLatch(1)
    val release   = new CountDownLatch(1)
    val finalized = new AtomicBoolean(false)
    val child     = runtime.effect.onCancel(
      ParIO.delay {
        started.countDown()
        release.await()
      }
    )(ParIO.delay(finalized.set(true)))

    try
      val fiber = runtime.unsafeRun(runtime.effect.start(observeOutcome(runtime)(child)))
      started.await(1, TimeUnit.SECONDS) shouldBe true

      runtime.unsafeRun(fiber.cancel)

      finalized.get() shouldBe true
      runtime.unsafeRun(fiber.join) shouldBe Outcome.Canceled()
    finally
      release.countDown()
      runtime.shutdown()
  }

  test("guarantee returns the original value after a successful finalizer") {
    val runtime   = testRuntime()
    val finalized = new AtomicBoolean(false)
    val program   = runtime.effect.guarantee(ParIO.pure(42))(ParIO.delay(finalized.set(true)))

    try
      runtime.unsafeRun(program) shouldBe 42
      finalized.get() shouldBe true
    finally runtime.shutdown()
  }

  test("guarantee rethrows the original error after a successful finalizer") {
    val runtime   = testRuntime()
    val original  = new RuntimeException("original")
    val finalized = new AtomicBoolean(false)
    val program   = runtime.effect.guarantee(ParIO.raiseError[Int](original))(ParIO.delay(finalized.set(true)))

    try
      val thrown = intercept[RuntimeException](runtime.unsafeRun(program))
      thrown shouldBe original
      finalized.get() shouldBe true
    finally runtime.shutdown()
  }

  test("guarantee suppresses finalizer failure when both effect and finalizer fail") {
    val runtime        = testRuntime()
    val original       = new RuntimeException("original")
    val finalizerError = new RuntimeException("finalizer")
    val program        = runtime.effect.guarantee(ParIO.raiseError[Int](original))(ParIO.raiseError(finalizerError))

    try
      val thrown = intercept[RuntimeException](runtime.unsafeRun(program))
      thrown shouldBe original
      thrown.getSuppressed.exists(_ eq finalizerError) shouldBe true
    finally runtime.shutdown()
  }

  test("guarantee runs finalizer after interruption with interrupt flag temporarily cleared") {
    val runtime                 = testRuntime()
    val finalizerSawInterrupted = new AtomicBoolean(true)
    val original                = new InterruptedException("interrupted")
    val program                 =
      runtime.effect.guarantee(
        ParIO.delay {
          Thread.currentThread().interrupt()
          throw original
        }
      )(
        ParIO.delay(finalizerSawInterrupted.set(Thread.currentThread().isInterrupted))
      )

    try
      val thrown = intercept[InterruptedException](runtime.unsafeRun(program))
      thrown shouldBe original
      finalizerSawInterrupted.get() shouldBe false
      Thread.currentThread().isInterrupted shouldBe true
    finally
      Thread.interrupted()
      runtime.shutdown()
  }

  test("guarantee fails with the finalizer error when the effect succeeds") {
    val runtime        = testRuntime()
    val finalizerError = new RuntimeException("finalizer")
    val program        = runtime.effect.guarantee(ParIO.pure(42))(ParIO.raiseError(finalizerError))

    try
      val thrown = intercept[RuntimeException](runtime.unsafeRun(program))
      thrown shouldBe finalizerError
      thrown.getSuppressed.toSeq shouldBe empty
    finally runtime.shutdown()
  }

  test("guarantee runs the finalizer before cancellation completes") {
    val runtime   = testRuntime()
    val started   = new CountDownLatch(1)
    val release   = new CountDownLatch(1)
    val finalized = new AtomicBoolean(false)
    val program   = runtime.effect.guarantee(
      ParIO.delay {
        started.countDown()
        release.await()
        ()
      }
    )(ParIO.delay(finalized.set(true)))

    try
      val fiber = runtime.unsafeRun(runtime.effect.start(program))
      started.await(1, TimeUnit.SECONDS) shouldBe true

      runtime.unsafeRun(fiber.cancel)

      finalized.get() shouldBe true
      runtime.unsafeRun(fiber.join) shouldBe Outcome.Canceled()
    finally
      release.countDown()
      runtime.shutdown()
  }

  test("cancel completes a fiber that has not started so join cannot hang") {
    val runtime      = testRuntime(asyncSize = 1)
    val started      = new CountDownLatch(1)
    val release      = new CountDownLatch(1)
    val joinExecutor = Executors.newSingleThreadExecutor()

    try
      val occupied = runtime.unsafeRun(
        runtime.effect.start(
          ParIO.delay {
            started.countDown()
            release.await()
            ()
          }
        )
      )
      started.await(1, TimeUnit.SECONDS) shouldBe true

      val pending = runtime.unsafeRun(runtime.effect.start(ParIO.delay(42)))
      runtime.unsafeRun(pending.cancel)

      val joinTask = joinExecutor.submit(new Callable[Outcome[Int]]:
        override def call(): Outcome[Int] =
          runtime.unsafeRun(pending.join))

      joinTask.get(1, TimeUnit.SECONDS) shouldBe Outcome.Canceled()

      release.countDown()
      runtime.unsafeRun(occupied.join)
    finally
      release.countDown()
      joinExecutor.shutdownNow()
      runtime.shutdown()
  }

  test("cancel after fiber completion does not interrupt a reused async pool thread") {
    val runtime     = testRuntime(asyncSize = 1)
    val started     = new CountDownLatch(1)
    val release     = new CountDownLatch(1)
    val interrupted = new AtomicBoolean(false)

    try
      val completed = runtime.unsafeRun(runtime.effect.start(ParIO.delay(Thread.currentThread().getName)))
      succeededValue(runtime.unsafeRun(completed.join)) should startWith("test-async-")

      val running = runtime.unsafeRun(
        runtime.effect.start(
          ParIO.delay {
            started.countDown()
            try release.await(2, TimeUnit.SECONDS)
            catch case _: InterruptedException => interrupted.set(true)
            ()
          }
        )
      )
      started.await(1, TimeUnit.SECONDS) shouldBe true

      runtime.unsafeRun(completed.cancel)
      Thread.sleep(100)
      interrupted.get() shouldBe false

      release.countDown()
      runtime.unsafeRun(running.join)
    finally
      release.countDown()
      runtime.shutdown()
  }

  test("race restores interrupt status and cancels both tasks when caller is interrupted") {
    val runtime        = testRuntime(asyncSize = 2)
    val raceExecutor   = Executors.newSingleThreadExecutor()
    val raceThread     = new AtomicReference[Thread]()
    val leftStarted    = new CountDownLatch(1)
    val rightStarted   = new CountDownLatch(1)
    val releaseLosers  = new CountDownLatch(1)
    val leftCancelled  = new AtomicBoolean(false)
    val rightCancelled = new AtomicBoolean(false)

    try
      val interrupted = raceExecutor.submit(new Callable[Boolean]:
        override def call(): Boolean =
          raceThread.set(Thread.currentThread())
          try
            runtime.unsafeRun(
              runtime.effect.race(
                ParIO.delay {
                  leftStarted.countDown()
                  try releaseLosers.await()
                  catch case _: InterruptedException => leftCancelled.set(true)
                  "left"
                },
                ParIO.delay {
                  rightStarted.countDown()
                  try releaseLosers.await()
                  catch case _: InterruptedException => rightCancelled.set(true)
                  "right"
                }
              )
            )
            false
          catch
            case _: InterruptedException =>
              Thread.currentThread().isInterrupted)

      leftStarted.await(1, TimeUnit.SECONDS) shouldBe true
      rightStarted.await(1, TimeUnit.SECONDS) shouldBe true
      Option(raceThread.get()).foreach(_.interrupt())

      interrupted.get(1, TimeUnit.SECONDS) shouldBe true
      eventuallyCancelled(leftCancelled, rightCancelled)
    finally
      releaseLosers.countDown()
      raceExecutor.shutdownNow()
      runtime.shutdown()
  }

  test("a canceled participant does not win a race") {
    val runtime      = testRuntime()
    val leftCanceled = new CountDownLatch(1)
    val left         = runtime.effect.onCancel(runtime.effect.canceled)(ParIO.delay(leftCanceled.countDown()))
    val right        = ParIO.delay {
      if !leftCanceled.await(1, TimeUnit.SECONDS) then throw new IllegalStateException("left branch did not cancel")
      42
    }

    try runtime.unsafeRun(runtime.effect.race(left, right)) shouldBe Right(42)
    finally runtime.shutdown()
  }

  test("a race is canceled when both participants cancel") {
    val runtime   = testRuntime()
    val finalized = new AtomicBoolean(false)
    val race      = runtime.effect.onCancel(
      runtime.effect.race(runtime.effect.canceled, runtime.effect.canceled)
    )(ParIO.delay(finalized.set(true)))

    try
      intercept[CancellationException](runtime.unsafeRun(race))
      finalized.get() shouldBe true
    finally runtime.shutdown()
  }

  test("a failed participant terminates the race and cancels the other participant") {
    val runtime        = testRuntime()
    val error          = new RuntimeException("boom")
    val loserStarted   = new CountDownLatch(1)
    val loserFinalized = new AtomicBoolean(false)
    val loser          = ParIO
      .delay {
        loserStarted.countDown()
        Thread.sleep(10.seconds.toMillis)
      }
      .onCancel(ParIO.delay(loserFinalized.set(true)))
    val failed = ParIO.delay {
      if !loserStarted.await(1, TimeUnit.SECONDS) then throw new IllegalStateException("loser did not start")
      throw error
    }

    try
      intercept[RuntimeException](runtime.unsafeRun(runtime.effect.race(failed, loser))) shouldBe error
      loserFinalized.get() shouldBe true
    finally runtime.shutdown()
  }

  test("parallel.par fails fast when one effect raises") {
    val runtime = testRuntime()
    val boom    = new RuntimeException("boom")

    try
      val startedAt = System.nanoTime()
      val thrown    = intercept[RuntimeException] {
        runtime.unsafeRun(
          runtime.parallel.par(
            Seq(
              ParIO.delay {
                Thread.sleep(10.seconds.toMillis)
                ()
              },
              ParIO.raiseError[Unit](boom)
            )
          )
        )
      }

      thrown shouldBe boom
      (System.nanoTime() - startedAt).nanos should be < 2.seconds
    finally runtime.shutdown()
  }

  test("onCancel preserves a successful result without running the finalizer") {
    val runtime   = testRuntime()
    val finalized = new AtomicBoolean(false)
    val program   = runtime.effect.onCancel(ParIO.pure(42))(ParIO.delay(finalized.set(true)))

    try
      runtime.unsafeRun(program) shouldBe 42
      finalized.get() shouldBe false
    finally runtime.shutdown()
  }

  test("onCancel does not run for an ordinary failure") {
    val runtime   = testRuntime()
    val original  = new InterruptedException("not a cancellation request")
    val finalized = new AtomicBoolean(false)
    val program   = runtime.effect
      .onCancel(ParIO.raiseError[Int](original))(ParIO.delay(finalized.set(true)))
      .handleErrorWith(error => if error eq original then ParIO.pure(42) else ParIO.raiseError(error))

    try
      runtime.unsafeRun(program) shouldBe 42
      finalized.get() shouldBe false
    finally runtime.shutdown()
  }

  test("canceled runs cancellation finalizers and skips the remaining computation") {
    val runtime                 = testRuntime()
    val finalized               = new AtomicBoolean(false)
    val remainingComputationRan = new AtomicBoolean(false)
    val cancellationRecovered   = new AtomicBoolean(false)
    val program                 = runtime.effect
      .onCancel(runtime.effect.canceled)(ParIO.delay(finalized.set(true)))
      .flatMap(_ => ParIO.delay(remainingComputationRan.set(true)))
      .handleErrorWith(_ => ParIO.delay(cancellationRecovered.set(true)))

    try
      intercept[CancellationException](runtime.unsafeRun(program))
      finalized.get() shouldBe true
      remainingComputationRan.get() shouldBe false
      cancellationRecovered.get() shouldBe false
    finally runtime.shutdown()
  }

  test("onCancel runs nested finalizers in order and skips the remaining computation") {
    val runtime                 = testRuntime()
    val started                 = new CountDownLatch(1)
    val release                 = new CountDownLatch(1)
    val innerFinalized          = new CountDownLatch(1)
    val outerFinalized          = new CountDownLatch(1)
    val innerSawInterrupted     = new AtomicBoolean(true)
    val outerSawInnerFinalized  = new AtomicBoolean(false)
    val remainingComputationRan = new AtomicBoolean(false)
    val cancellationRecovered   = new AtomicBoolean(false)
    val work                    = ParIO
      .delay {
        started.countDown()
        release.await()
        ()
      }
      .onCancel(
        ParIO.delay {
          innerSawInterrupted.set(Thread.currentThread().isInterrupted)
          Thread.sleep(1)
          innerFinalized.countDown()
        }
      )
      .flatMap(_ => ParIO.delay(remainingComputationRan.set(true)))
      .onCancel(
        ParIO.delay {
          outerSawInnerFinalized.set(innerFinalized.getCount == 0L)
          outerFinalized.countDown()
        }
      )
      .handleErrorWith(_ => ParIO.delay(cancellationRecovered.set(true)))

    try
      val fiber = runtime.unsafeRun(runtime.effect.start(work))
      started.await(1, TimeUnit.SECONDS) shouldBe true

      runtime.unsafeRun(fiber.cancel)

      innerFinalized.getCount shouldBe 0L
      outerFinalized.getCount shouldBe 0L
      innerSawInterrupted.get() shouldBe false
      outerSawInnerFinalized.get() shouldBe true
      remainingComputationRan.get() shouldBe false
      cancellationRecovered.get() shouldBe false
      runtime.unsafeRun(fiber.join) shouldBe Outcome.Canceled()
    finally
      release.countDown()
      runtime.shutdown()
  }

  test("uncancellable defers self-cancellation until the region exits") {
    val runtime          = testRuntime()
    val insideRegionRan  = new AtomicBoolean(false)
    val outsideRegionRan = new AtomicBoolean(false)
    val program          = runtime.effect
      .uncancellable { _ =>
        runtime.effect.canceled.flatMap(_ => ParIO.delay(insideRegionRan.set(true)))
      }
      .flatMap(_ => ParIO.delay(outsideRegionRan.set(true)))

    try
      runtime.unsafeRun(observeOutcome(runtime)(program)) shouldBe Outcome.Canceled()
      insideRegionRan.get() shouldBe true
      outsideRegionRan.get() shouldBe false
    finally runtime.shutdown()
  }

  test("uncancellable completes normally when cancellation was not requested") {
    val runtime = testRuntime()

    try
      runtime.unsafeRun(runtime.effect.uncancellable(_ => ParIO.pure(42))) shouldBe 42
    finally runtime.shutdown()
  }

  test("external cancellation waits for an uncancellable region to exit") {
    val runtime          = testRuntime()
    val cancelExecutor   = Executors.newSingleThreadExecutor()
    val started          = new CountDownLatch(1)
    val release          = new CountDownLatch(1)
    val interrupted      = new AtomicBoolean(false)
    val insideRegionRan  = new AtomicBoolean(false)
    val outsideRegionRan = new AtomicBoolean(false)
    val program          = runtime.effect
      .uncancellable { _ =>
        ParIO.delay {
          started.countDown()
          try release.await()
          catch
            case error: InterruptedException =>
              interrupted.set(true)
              throw error
          insideRegionRan.set(true)
        }
      }
      .flatMap(_ => ParIO.delay(outsideRegionRan.set(true)))

    try
      val fiber = runtime.unsafeRun(runtime.effect.start(program))
      started.await(1, TimeUnit.SECONDS) shouldBe true

      val cancellation = cancelExecutor.submit(new Callable[Unit]:
        override def call(): Unit = runtime.unsafeRun(fiber.cancel))

      Thread.sleep(100)
      cancellation.isDone shouldBe false

      release.countDown()
      cancellation.get(1, TimeUnit.SECONDS)

      interrupted.get() shouldBe false
      insideRegionRan.get() shouldBe true
      outsideRegionRan.get() shouldBe false
      runtime.unsafeRun(fiber.join) shouldBe Outcome.Canceled()
    finally
      release.countDown()
      cancelExecutor.shutdownNow()
      runtime.shutdown()
  }

  test("poll restores cancellation for its source") {
    val runtime       = testRuntime()
    val started       = new CountDownLatch(1)
    val release       = new CountDownLatch(1)
    val finalized     = new AtomicBoolean(false)
    val afterPollRan  = new AtomicBoolean(false)
    val polledProgram = runtime.effect.onCancel(
      ParIO.delay {
        started.countDown()
        release.await()
        ()
      }
    )(ParIO.delay(finalized.set(true)))
    val program = runtime.effect.uncancellable { poll =>
      poll(polledProgram).flatMap(_ => ParIO.delay(afterPollRan.set(true)))
    }

    try
      val fiber = runtime.unsafeRun(runtime.effect.start(program))
      started.await(1, TimeUnit.SECONDS) shouldBe true

      runtime.unsafeRun(fiber.cancel)

      finalized.get() shouldBe true
      afterPollRan.get() shouldBe false
      runtime.unsafeRun(fiber.join) shouldBe Outcome.Canceled()
    finally
      release.countDown()
      runtime.shutdown()
  }

  test("cancellation is masked again after a successful poll") {
    val runtime          = testRuntime()
    val cancelExecutor   = Executors.newSingleThreadExecutor()
    val started          = new CountDownLatch(1)
    val release          = new CountDownLatch(1)
    val interrupted      = new AtomicBoolean(false)
    val insideRegionRan  = new AtomicBoolean(false)
    val outsideRegionRan = new AtomicBoolean(false)
    val program          = runtime.effect
      .uncancellable { poll =>
        poll(ParIO.unit).flatMap { _ =>
          ParIO.delay {
            started.countDown()
            try release.await()
            catch
              case error: InterruptedException =>
                interrupted.set(true)
                throw error
            insideRegionRan.set(true)
          }
        }
      }
      .flatMap(_ => ParIO.delay(outsideRegionRan.set(true)))

    try
      val fiber = runtime.unsafeRun(runtime.effect.start(program))
      started.await(1, TimeUnit.SECONDS) shouldBe true

      val cancellation = cancelExecutor.submit(new Callable[Unit]:
        override def call(): Unit = runtime.unsafeRun(fiber.cancel))

      Thread.sleep(100)
      cancellation.isDone shouldBe false

      release.countDown()
      cancellation.get(1, TimeUnit.SECONDS)

      interrupted.get() shouldBe false
      insideRegionRan.get() shouldBe true
      outsideRegionRan.get() shouldBe false
      runtime.unsafeRun(fiber.join) shouldBe Outcome.Canceled()
    finally
      release.countDown()
      cancelExecutor.shutdownNow()
      runtime.shutdown()
  }

  test("an error outside poll observes the reinstated cancellation mask") {
    val runtime              = testRuntime()
    val continuedAfterCancel = new AtomicBoolean(false)
    val error                = new RuntimeException("boom")
    val program              = runtime.effect.uncancellable { poll =>
      poll(ParIO.raiseError[Unit](error)).handleErrorWith { _ =>
        runtime.effect.canceled.flatMap(_ => ParIO.delay(continuedAfterCancel.set(true)))
      }
    }

    try
      runtime.unsafeRun(observeOutcome(runtime)(program)) shouldBe Outcome.Canceled()
      continuedAfterCancel.get() shouldBe true
    finally runtime.shutdown()
  }

  test("an error removes an uncancellable region before an outer handler runs") {
    val runtime              = testRuntime()
    val continuedAfterCancel = new AtomicBoolean(false)
    val error                = new RuntimeException("boom")
    val program              = runtime.effect
      .uncancellable[Unit](_ => throw error)
      .handleErrorWith { _ =>
        runtime.effect.canceled.flatMap(_ => ParIO.delay(continuedAfterCancel.set(true)))
      }

    try
      runtime.unsafeRun(observeOutcome(runtime)(program)) shouldBe Outcome.Canceled()
      continuedAfterCancel.get() shouldBe false
    finally runtime.shutdown()
  }

  test("a throwing error handler does not leak its enclosing cancellation mask") {
    val runtime              = testRuntime()
    val firstError           = new RuntimeException("first")
    val handlerError         = new RuntimeException("handler")
    val continuedAfterCancel = new AtomicBoolean(false)
    val failed               = runtime.effect.uncancellable { _ =>
      ParIO.raiseError[Unit](firstError).handleErrorWith(_ => throw handlerError)
    }
    val program = observeOutcome(runtime)(failed).flatMap {
      case Outcome.Failed(error) if error eq handlerError =>
        runtime.effect.canceled.flatMap(_ => ParIO.delay(continuedAfterCancel.set(true)))
      case outcome =>
        ParIO.raiseError(new IllegalStateException(s"unexpected outcome: $outcome"))
    }

    try
      runtime.unsafeRun(observeOutcome(runtime)(program)) shouldBe Outcome.Canceled()
      continuedAfterCancel.get() shouldBe false
    finally runtime.shutdown()
  }

  test("observing a child materializes its cancellation inside an uncancellable region") {
    val runtime     = testRuntime()
    val observation = runtime.effect.uncancellable { _ =>
      observeOutcome(runtime)(runtime.effect.canceled)
    }

    try
      runtime.unsafeRun(observation) shouldBe Outcome.Canceled()
    finally runtime.shutdown()
  }

  test("an outer poll does not open a nested uncancellable region") {
    val runtime   = testRuntime()
    val continued = new AtomicBoolean(false)
    val program   = runtime.effect.uncancellable { outerPoll =>
      runtime.effect.uncancellable { _ =>
        outerPoll(runtime.effect.canceled).flatMap(_ => ParIO.delay(continued.set(true)))
      }
    }

    try
      runtime.unsafeRun(observeOutcome(runtime)(program)) shouldBe Outcome.Canceled()
      continued.get() shouldBe true
    finally runtime.shutdown()
  }

  test("race completes the losing branch cancellation before returning") {
    val runtime        = testRuntime()
    val loserStarted   = new CountDownLatch(1)
    val releaseLoser   = new CountDownLatch(1)
    val loserFinalized = new AtomicBoolean(false)
    val winner         = ParIO.delay {
      loserStarted.await()
      "winner"
    }
    val loser = ParIO
      .delay {
        loserStarted.countDown()
        releaseLoser.await()
        "loser"
      }
      .onCancel(ParIO.delay(loserFinalized.set(true)))

    try
      runtime.unsafeRun(runtime.effect.race(winner, loser)) shouldBe Left("winner")
      loserFinalized.get() shouldBe true
    finally
      releaseLoser.countDown()
      runtime.shutdown()
  }

  private def eventuallyCancelled(leftCancelled: AtomicBoolean, rightCancelled: AtomicBoolean): Unit =
    val deadline = System.nanoTime() + 1.second.toNanos
    while System.nanoTime() < deadline && !(leftCancelled.get() && rightCancelled.get()) do Thread.sleep(10)

    leftCancelled.get() shouldBe true
    rightCancelled.get() shouldBe true
