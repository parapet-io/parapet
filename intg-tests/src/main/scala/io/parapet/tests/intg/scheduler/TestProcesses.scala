package io.parapet.tests.intg.scheduler

import io.parapet.core.Process
import io.parapet.testutils.EventStore

import scala.concurrent.duration.FiniteDuration

/** Factory helpers for the test processes.
  */
object TestProcesses {

  def dummy[F[_]]: Process[F] = new Process[F] {
    import dsl._
    override val handle: Receive = { case _ => unit }
  }

  /** Process that stores every [[TestEvent]] into `eventStore`.
    *
    * @param eventStore
    *   sink for observed events.
    * @param time
    *   handler delay. [[TaskProcessingTime.instant]] means zero delay.
    * @param blockingHandler
    *   when true, wraps the body in `blocking { ... }`.
    */
  def create[F[_]](
      eventStore: EventStore[F, TestEvent],
      time: FiniteDuration = TaskProcessingTime.instant.time,
      blockingHandler: Boolean
  ): Process[F] =
    new Process[F] {
      import dsl._
      val handle: Receive = { case e: TestEvent =>
        val body = delay(time) ++ eval(eventStore.add(ref, e))
        if (blockingHandler) blocking(body) else body
      }
    }

  /** Creates `n` processes.
    *
    * @param n
    *   total process count.
    * @param workload
    *   processing-time distribution. See [[WorkloadProfile]].
    * @param eventStore
    *   sink shared by all processes.
    * @param blockingRatio
    *   fraction of processes using `blocking { ... }`.
    */
  def createAll[F[_]](
      n: Int,
      workload: WorkloadProfile,
      eventStore: EventStore[F, TestEvent],
      blockingRatio: Double = 0.0
  ): Array[Process[F]] = {
    val processes = new Array[Process[F]](n)
    val blocking  = blockingIndexes(n, blockingRatio)
    (0 until n).foreach { i =>
      processes(i) = create(eventStore, WorkloadProfile.timeFor(workload, i, n).time, blocking.contains(i))
    }
    processes
  }

  /** Picks the indexes in `[0, n)` that should use `blocking { ... }`, spread on a uniform stride.
    *
    * Algorithm:
    *   - `bN = round(n * blockingRatio)`, clamped to `[0, n]` - how many processes should block.
    *   - `stride = max(1, n / bN)` - how far apart they should sit.
    *   - Walk `0, stride, 2*stride, ...` and take the first `bN` hits.
    */
  private def blockingIndexes(n: Int, blockingRatio: Double): Set[Int] = {
    val bN = math.min(n, math.max(0, (n * blockingRatio).round.toInt))
    if (bN == 0) Set.empty
    else {
      val stride = math.max(1, n / bN)
      (0 until n by stride).take(bN).toSet
    }
  }
}
