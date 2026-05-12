package io.parapet.tests.intg.pario

import io.parapet.ProcessRef
import io.parapet.core.{Context, DslInterpreter, EventTransformers, Parallel, Parapet, Scheduler, SchedulerRuntime}
import io.parapet.core.Scheduler.SchedulerConfig
import io.parapet.effect.{
  Effect,
  ElasticPoolConfig,
  FixedPoolConfig,
  ParIO,
  ParIORuntime,
  ParIORuntimeConfig,
  TimerThreadPoolConfig
}
import io.parapet.tests.intg.scheduler.{
  TaskSubmitter,
  TestEvent,
  TestProcesses,
  WorkDistributionStrategy,
  WorkloadProfile
}
import io.parapet.testutils.EventStore
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.util.concurrent.{Callable, ExecutionException, Executors, TimeUnit, TimeoutException}
import scala.concurrent.duration.*

class SchedulerRegressionSpec extends AnyFunSuite:
  private def testRuntime(): ParIORuntime =
    new ParIORuntime(
      ParIORuntimeConfig(
        // Match the scheduler worker count so the reproducer exercises the starvation edge deterministically.
        scheduler = ElasticPoolConfig(
          coreSize = 2,
          maxSize = Int.MaxValue,
          keepAlive = 30.seconds,
          threadNamePrefix = "reg-scheduler"
        ),
        // Keep the public parallel pool equally small to prove scheduler workers do not occupy it.
        parallel = FixedPoolConfig(2, "reg-parallel"),
        // Keep async larger so the regression isolates submitter starvation on the parallel pool.
        async = FixedPoolConfig(6, "reg-async"),
        blocking = ElasticPoolConfig(
          coreSize = 0,
          maxSize = 32,
          keepAlive = 30.seconds,
          threadNamePrefix = "reg-blocking"
        ),
        race = ElasticPoolConfig(
          coreSize = 0,
          maxSize = 16,
          keepAlive = 30.seconds,
          threadNamePrefix = "reg-race"
        ),
        timer = TimerThreadPoolConfig(1, "reg-timer")
      )
    )

  private def unsafeRunWithTimeout(runtime: ParIORuntime, program: ParIO[Unit], timeout: FiniteDuration): Unit =
    val runner = Executors.newSingleThreadExecutor()
    val future = runner.submit(new Callable[Unit]:
      override def call(): Unit =
        runtime.unsafeRun(program))

    try future.get(timeout.toMillis, TimeUnit.MILLISECONDS)
    catch
      case error: ExecutionException if error.getCause != null =>
        throw error.getCause
      case _: TimeoutException =>
        fail(s"program did not complete within $timeout")
    finally
      runtime.shutdown()
      runner.shutdownNow()

  test("scheduler submitters still make progress when worker count matches the parallel pool size") {
    val runtime = testRuntime()

    given Effect[ParIO]           = runtime.effect
    given Parallel[ParIO]         = runtime.parallel
    given SchedulerRuntime[ParIO] = runtime.schedulerRuntime

    val eventStore = new EventStore[ParIO, TestEvent]
    val processes  = TestProcesses.createAll(2, WorkloadProfile.instant, eventStore)
    val tasks      = WorkDistributionStrategy.RoundRobin.createTasks(20, processes, numberOfSubmitters = 2)
    val config     = Parapet.ParConfig(
      processBufferSize = -1,
      schedulerConfig = SchedulerConfig(numberOfWorkers = 2),
      devMode = true,
      tracingEnabled = true,
      eventLogEnabled = true
    )

    val program =
      for
        context   <- Context[ParIO](config, io.parapet.core.EventStore.stub, EventTransformers.empty)
        scheduler <- Scheduler[ParIO](config.schedulerConfig, context, DslInterpreter(context))
        fiber     <- summon[Effect[ParIO]].start(scheduler.start)
        _         <- context.start(scheduler)
        _         <- context.registerAll(ProcessRef.SystemRef, processes.toList)
        _         <- TaskSubmitter.submitAll(scheduler, tasks, numberOfSubmitters = 2)
        _         <- eventStore.await0(tasks.size, fiber, delay = 10.millis, timeout = 3.seconds)
      yield ()

    unsafeRunWithTimeout(runtime, program, 5.seconds)
    eventStore.size shouldBe tasks.size
  }
