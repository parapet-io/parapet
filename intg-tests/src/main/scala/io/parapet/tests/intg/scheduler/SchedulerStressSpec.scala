package io.parapet.tests.intg.scheduler

import io.parapet.core.Scheduler.SchedulerConfig
import io.parapet.syntax.logger.*
import io.parapet.tests.intg.scheduler.SchedulerStressSpec
import io.parapet.tests.intg.scheduler.*
import io.parapet.tests.intg.scheduler.TaskProcessingTime.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import scala.util.Random

/** Randomized stress driver for the scheduler.
  *
  * Configuration via environment variables or `-D` system properties (env takes precedence):
  *
  *   - `SCHEDULER_STRESS_ITERATIONS` / `-Dscheduler.stress.iterations=N` - number of iterations. Default `5` so a
  *     default `sbt test` run only does a short smoke. The dedicated `schedulerStress` sbt task sets this to `0` for an
  *     unbounded loop (run forever until a failure or manual interruption).
  *   - `SCHEDULER_STRESS_SEED` / `-Dscheduler.stress.seed=N` - seed used to derive per-iteration seeds. Default:
  *     `System.nanoTime()`. Setting this to a previously reported value regenerates the same sequence of generated
  *     stress scenarios.
  *
  * When rerunning a failing scenario sequence, set the seed to the reported `baseSeed` and iterations to
  * `failedIteration + 1`. This does not guarantee the same failure will happen again because runtime timing and thread
  * scheduling remain nondeterministic.
  *
  * The stress loop lives in a dedicated `test` block so it is easy to filter with `-z "stress loop"`. This spec shares
  * [[SchedulerTestRunner]] with [[SchedulerCorrectnessSpec]] but does not inherit its `test(...)` blocks, so `sbt test`
  * does not run the baseline matrix twice.
  */
abstract class SchedulerStressSpec[F[_]] extends AnyFunSuite with SchedulerTestRunner[F] {

  test("scheduler stress loop") {
    def setting(envKey: String, propKey: String, default: => String): String =
      sys.env.get(envKey).orElse(sys.props.get(propKey)).getOrElse(default)

    val iterations = setting("SCHEDULER_STRESS_ITERATIONS", "scheduler.stress.iterations", "5").toLong
    val baseSeed   = setting("SCHEDULER_STRESS_SEED", "scheduler.stress.seed", System.nanoTime().toString).toLong
    val infinite   = iterations == 0L
    val rnd        = new Random(baseSeed)

    val banner =
      s"scheduler stress: baseSeed=$baseSeed iterations=${if (infinite) "infinite" else iterations.toString}"
    logger.info(banner)
    info(banner)

    var i = 0L
    while (infinite || i < iterations) {
      val iterSeed = rnd.nextLong()
      val spec     = SchedulerStressSpec.randomSpec(new Random(iterSeed), i)
      val prefix   = s"stress iter=$i iterSeed=$iterSeed baseSeed=$baseSeed"
      logger.info(s"$prefix starting: $spec")
      try run(spec)
      catch {
        case t: Throwable =>
          logger.error(s"$prefix failed: ${t.getMessage}")
          fail(
            s"scheduler stress FAILED. Rerun the same generated scenario sequence with: " +
              s"SCHEDULER_STRESS_SEED=$baseSeed SCHEDULER_STRESS_ITERATIONS=${i + 1}. " +
              s"This may not reproduce the exact same failure because scheduling remains nondeterministic. " +
              s"Iteration $i used iterSeed=$iterSeed, spec=$spec. Cause: ${t.getMessage}",
            t
          )
      }
      logger.info(s"$prefix ok")
      i += 1L
    }
  }

}

object SchedulerStressSpec {

  /** Generates a random [[StabilitySpec]] within bounds that keep an individual iteration below a few seconds so the
    * loop can rack up many iterations overnight without a single sample dominating.
    */
  def randomSpec(rnd: Random, i: Long): StabilitySpec = {
    val workers    = 1 + rnd.nextInt(12)
    val processes  = 1 + rnd.nextInt(200)
    val events     = 1 + rnd.nextInt(500)
    val submitters = 1 + rnd.nextInt(6)
    val blocking   = rnd.nextDouble() * 0.5
    val wds        =
      if (rnd.nextBoolean()) WorkDistributionStrategy.Random else WorkDistributionStrategy.Batch
    val slow = rnd.nextInt(3) match {
      case 0 => TaskProcessingTime.instant
      case 1 => range(1.millis, 10.millis)
      case _ => range(5.millis, 30.millis)
    }
    val workload = WorkloadProfile.TwoGroup(
      fraction = rnd.nextDouble(),
      first = TaskProcessingTime.instant,
      second = slow
    )

    // Batch explodes task count as `events * processes`. Cap processes when batch is picked so we
    // do not accidentally schedule millions of tasks in a single iteration.
    val cappedProcesses = if (wds == WorkDistributionStrategy.Batch) math.min(processes, 20) else processes
    val cappedEvents    = if (wds == WorkDistributionStrategy.Batch) math.min(events, 50) else events

    StabilitySpec(
      name = s"stress-$i",
      samples = 1,
      config = SchedulerConfig(workers),
      wds = wds,
      numberOfEvents = cappedEvents,
      numberOfProcesses = cappedProcesses,
      workload = workload,
      numberOfSubmitters = submitters,
      blockingRatio = blocking
    ).withDiagnostics
  }

}
