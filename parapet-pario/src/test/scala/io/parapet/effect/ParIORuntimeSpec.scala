package io.parapet.effect

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.util.concurrent.{Callable, CancellationException, CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.*

class ParIORuntimeSpec extends AnyFunSuite:
  private def testRuntime(asyncSize: Int = 2): ParIORuntime =
    new ParIORuntime(
      ParIORuntimeConfig(
        scheduler = FixedThreadPoolConfig(2, "test-scheduler"),
        parallel = FixedThreadPoolConfig(2, "test-parallel"),
        async = FixedThreadPoolConfig(asyncSize, "test-async"),
        blocking = BlockingThreadPoolConfig(
          coreSize = 0,
          maxSize = 4,
          keepAlive = 30.seconds,
          threadNamePrefix = "test-blocking"
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
      runtime.unsafeRun(fiber.join) should startWith("test-blocking-")
    finally runtime.shutdown()
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

      val joinTask = joinExecutor.submit(new Callable[CancellationException]:
        override def call(): CancellationException =
          intercept[CancellationException] {
            runtime.unsafeRun(pending.join)
          })

      joinTask.get(1, TimeUnit.SECONDS) shouldBe a[CancellationException]

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
      runtime.unsafeRun(completed.join) should startWith("test-async-")

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

  test("nested race timeout is not starved by occupied async workers") {
    val runtime  = testRuntime(asyncSize = 4)
    val release  = new CountDownLatch(1)
    val executor = Executors.newSingleThreadExecutor()

    try
      val neverCompletes = runtime.unsafeRun(
        runtime.effect.start(
          ParIO.blocking {
            release.await()
            ()
          }
        )
      )

      val timeout =
        runtime.effect
          .race(
            ParIO.sleep(10.seconds).map(_ => "slow"),
            ParIO.sleep(100.millis).map(_ => "timeout")
          )
          .map(_.fold(identity, identity))

      val program =
        runtime.effect.race(
          timeout,
          neverCompletes.join.map(_ => "joined")
        )

      val result = executor.submit(new Callable[Either[String, String]]:
        override def call(): Either[String, String] =
          runtime.unsafeRun(program))

      result.get(1, TimeUnit.SECONDS) shouldBe Left("timeout")
    finally
      release.countDown()
      executor.shutdownNow()
      runtime.shutdown()
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

  private def eventuallyCancelled(leftCancelled: AtomicBoolean, rightCancelled: AtomicBoolean): Unit =
    val deadline = System.nanoTime() + 1.second.toNanos
    while System.nanoTime() < deadline && !(leftCancelled.get() && rightCancelled.get()) do Thread.sleep(10)

    leftCancelled.get() shouldBe true
    rightCancelled.get() shouldBe true
