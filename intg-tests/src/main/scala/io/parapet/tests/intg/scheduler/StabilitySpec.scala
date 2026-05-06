package io.parapet.tests.intg.scheduler

import io.parapet.core.Scheduler.SchedulerConfig

/** Declarative description of a single scheduler-correctness scenario.
  *
  * A [[StabilitySpec]] is a value; the runner interprets it. Splitting configuration from execution keeps the
  * correctness matrix and the stress-loop's randomized specs on the same abstraction: both produce [[StabilitySpec]]s
  * and both feed them through [[io.parapet.tests.intg.scheduler.SchedulerTestRunner.run]].
  *
  * Fields:
  *
  * @param name
  *   short, unique identifier. Surfaces in log MDC, failure report filenames, and reproducer messages.
  * @param samples
  *   number of times to repeat this spec. Each sample rebuilds a fresh scheduler / context / event store.
  * @param config
  *   scheduler configuration (worker count, signal-queue topology, mailbox slice).
  * @param wds
  *   how tasks are distributed across receivers. See [[WorkDistributionStrategy]].
  * @param numberOfEvents
  *   number of events to submit. For [[WorkDistributionStrategy.Batch]] this is the batch size per receiver, so total
  *   tasks = `numberOfEvents * numberOfProcesses`.
  * @param numberOfProcesses
  *   receivers registered in the context.
  * @param workload
  *   per-receiver processing-time distribution. See [[WorkloadProfile]] for the available shapes (uniform, two-group
  *   split, …).
  * @param numberOfSubmitters
  *   number of concurrent fibers feeding the scheduler. Each fiber submits events tagged with its own `submitterId`.
  * @param blockingRatio
  *   fraction of processes whose handler is wrapped in `blocking { ... }`.
  * @param devMode
  *   enables verbose DEBUG-level scheduler logging. Routed to the file appender configured in `logback-test.xml` so the
  *   console stays quiet. Default off to keep `sbt test` fast.
  * @param tracingEnabled
  *   propagates [[io.parapet.core.ExecutionTrace]] ids through envelopes so each scheduler hop logs a `traceId:...`
  *   suffix. Only useful when `devMode` is also on.
  * @param eventLogEnabled
  *   records every delivered envelope into an in-memory [[io.parapet.core.EventLog]] graph. The runtime does not
  *   currently persist this to disk, but it can be inspected in-process on failure.
  * @param enabled
  *   when false, the runner skips this spec. Useful for quickly disabling a scenario during investigation without
  *   deleting its definition.
  */
final case class StabilitySpec(
    name: String,
    samples: Int = 1,
    config: SchedulerConfig,
    wds: WorkDistributionStrategy,
    numberOfEvents: Int,
    numberOfProcesses: Int,
    workload: WorkloadProfile,
    numberOfSubmitters: Int = 1,
    blockingRatio: Double = 0.0,
    devMode: Boolean = false,
    tracingEnabled: Boolean = false,
    eventLogEnabled: Boolean = false,
    enabled: Boolean = true
) {

  /** Returns a copy of this spec with [[enabled]] set to `false`. */
  def disable: StabilitySpec = this.copy(enabled = false)

  /** Enables all diagnostic.
    */
  def withDiagnostics: StabilitySpec =
    copy(devMode = true, tracingEnabled = true, eventLogEnabled = true)
}

object StabilitySpec {

  /** Empty spec.
    */
  val default: StabilitySpec = StabilitySpec(
    name = "default",
    config = SchedulerConfig(numberOfWorkers = 1),
    wds = WorkDistributionStrategy.RoundRobin,
    numberOfEvents = 0,
    numberOfProcesses = 0,
    workload = WorkloadProfile.instant
  )
}
