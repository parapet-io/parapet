package io.parapet.core

import com.typesafe.scalalogging.StrictLogging
import io.parapet.runtime.Scheduler.SchedulerConfig
import io.parapet.journal.JournalConfig
import io.parapet.runtime.Scheduler
import io.parapet.snapshot.SnapshotConfig

/** Top-level constants and tuning knobs for the parapet runtime.
  *
  * Most users don't import this directly - the [[ParConfig]] case class is reached via [[io.parapet.ParApp.config]]
  * when an application needs to override defaults.
  */
object Parapet extends StrictLogging:

  /** Bundle of runtime configuration values supplied to [[io.parapet.ParApp]].
    *
    * @param processBufferSize
    *   default mailbox capacity for processes that don't override [[Process.bufferSize]]; `-1` means unbounded.
    * @param schedulerConfig
    *   [[Scheduler]] tuning (worker thread count, etc.).
    * @param devMode
    *   when `true` enables verbose runtime logging useful while developing.
    * @param snapshot
    *   snapshotting / recovery configuration; disabled by default.
    */
  final case class ParConfig(
      processBufferSize: Int,
      schedulerConfig: SchedulerConfig,
      devMode: Boolean = false,
      snapshot: SnapshotConfig = SnapshotConfig.disabled,
      journal: JournalConfig = JournalConfig()
  ):
    /** Sets the default per-process mailbox capacity. */
    def withProcessBufferSize(value: Int): ParConfig =
      copy(processBufferSize = value)

    /** Sets the number of [[Scheduler]] worker threads. */
    def withWorkerCount(value: Int): ParConfig =
      copy(schedulerConfig = schedulerConfig.copy(numberOfWorkers = value))

    /** Enables verbose dev-mode logging. */
    def withDevMode: ParConfig =
      copy(devMode = true)

    /** Enables snapshotting with data under `dataDir`. */
    def withSnapshots(dataDir: String): ParConfig =
      copy(snapshot = snapshot.copy(enabled = true, dataDir = dataDir))

    /** Enables the delivery journal with data under `dataDir`. */
    def withJournal(dataDir: String): ParConfig =
      copy(journal = journal.copy(enabled = true, dataDir = dataDir))

  object ParConfig:
    /** Sensible defaults: unbounded process queues, one worker per CPU. */
    val default: ParConfig =
      ParConfig(
        processBufferSize = -1,
        schedulerConfig = SchedulerConfig.default
      )
