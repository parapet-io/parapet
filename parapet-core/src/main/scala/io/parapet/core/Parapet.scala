package io.parapet.core

import com.typesafe.scalalogging.StrictLogging
import io.parapet.core.Scheduler.SchedulerConfig

/** Top-level constants and tuning knobs for the parapet runtime.
  *
  * Most users don't import this directly - the [[ParConfig]] case class is reached via [[io.parapet.ParApp.config]]
  * when an application needs to override defaults.
  */
object Parapet extends StrictLogging:

  /** Snapshotting / recovery configuration (see `dev-docs/snapshot.md`).
    *
    * @param enabled
    *   master switch. When on, the runtime periodically snapshots [[io.parapet.core.snapshot.Snapshotable]] processes
    *   to `dataDir` as they run.
    * @param dataDir
    *   directory holding the per-process snapshot files; must survive restarts for recovery to be useful.
    * @param maxEventsPerSnapshot
    *   cadence ceiling: a process that keeps receiving events without draining its mailbox is snapshotted at least
    *   every this many deliveries. Bounds how many events a restore/replay has to re-fold.
    * @param maxSnapshotIntervalMillis
    *   time-based cadence for a continuously-busy process: while it has unsnapshotted state and never drains its
    *   mailbox, it is snapshotted at least this often (wall-clock). Bounds how much real-time work a crash can lose.
    *   `0` disables the time trigger, leaving cadence purely event-count driven.
    * @param queueCapacity
    *   capacity of the background snapshot-writer queue; a snapshot enqueued when it is full is dropped (best-effort).
    */
  final case class SnapshotConfig(
      enabled: Boolean = false,
      dataDir: String = "parapet-snapshots",
      maxEventsPerSnapshot: Int = 1000,
      maxSnapshotIntervalMillis: Long = 0,
      queueCapacity: Int = 1024
  )

  object SnapshotConfig:
    val disabled: SnapshotConfig = SnapshotConfig()

  /** Bundle of runtime configuration values supplied to [[io.parapet.ParApp]].
    *
    * @param processBufferSize
    *   default mailbox capacity for processes that don't override [[Process.bufferSize]]; `-1` means unbounded.
    * @param schedulerConfig
    *   [[Scheduler]] tuning (worker thread count, etc.).
    * @param devMode
    *   when `true` enables verbose runtime logging useful while developing.
    * @param eventLogEnabled
    *   when `true` records every delivered envelope to an in-memory [[EventLog]]; primarily for replay/debugging.
    * @param snapshot
    *   snapshotting / recovery configuration; disabled by default.
    */
  final case class ParConfig(
      processBufferSize: Int,
      schedulerConfig: SchedulerConfig,
      devMode: Boolean = false,
      eventLogEnabled: Boolean = false,
      snapshot: SnapshotConfig = SnapshotConfig.disabled,
      journal: io.parapet.core.journal.JournalConfig = io.parapet.core.journal.JournalConfig()
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

    /** Enables the in-memory event log. */
    def enableEventLog: ParConfig =
      copy(eventLogEnabled = true)

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
