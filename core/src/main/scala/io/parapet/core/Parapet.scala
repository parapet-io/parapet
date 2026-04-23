package io.parapet.core

import com.typesafe.scalalogging.StrictLogging
import io.parapet.core.Scheduler.SchedulerConfig

/** Top-level constants and tuning knobs for the parapet runtime.
  *
  * Most users don't import this directly — the [[ParConfig]] case class is reached via
  * [[io.parapet.ParApp.config]] when an application needs to override defaults.
  */
object Parapet extends StrictLogging:
  /** Library version string, surfaced in logs and diagnostics. */
  val Version = "0.0.1-RC7"

  /** Bundle of runtime configuration values supplied to [[io.parapet.ParApp]].
    *
    * @param processBufferSize  default mailbox capacity for processes that don't override
    *                           [[Process.bufferSize]]; `-1` means unbounded.
    * @param schedulerConfig    [[Scheduler]] tuning (worker thread count, etc.).
    * @param devMode            when `true` enables verbose runtime logging useful while
    *                           developing.
    * @param tracingEnabled     when `true` propagates [[ExecutionTrace]] ids through
    *                           envelopes for cross-process causal tracing.
    * @param eventLogEnabled    when `true` records every delivered envelope to an
    *                           in-memory [[EventLog]]; primarily for replay/debugging.
    */
  final case class ParConfig(
      processBufferSize: Int,
      schedulerConfig: SchedulerConfig,
      devMode: Boolean = false,
      tracingEnabled: Boolean = false,
      eventLogEnabled: Boolean = false
  ):
    /** Sets the default per-process mailbox capacity. */
    def withProcessBufferSize(value: Int): ParConfig =
      copy(processBufferSize = value)

    /** Sets the number of [[Scheduler]] worker threads. */
    def withWorkerCount(value: Int): ParConfig =
      copy(schedulerConfig = schedulerConfig.copy(numberOfWorkers = value))

    /** Enables [[ExecutionTrace]] propagation. */
    def enableTracing: ParConfig =
      copy(tracingEnabled = true)

    /** Enables verbose dev-mode logging. */
    def withDevMode: ParConfig =
      copy(devMode = true)

    /** Enables the in-memory event log. */
    def enableEventLog: ParConfig =
      copy(eventLogEnabled = true)

  object ParConfig:
    /** Sensible defaults: unbounded process queues, one worker per CPU, no tracing. */
    val default: ParConfig =
      ParConfig(
        processBufferSize = -1,
        schedulerConfig = SchedulerConfig.default
      )
