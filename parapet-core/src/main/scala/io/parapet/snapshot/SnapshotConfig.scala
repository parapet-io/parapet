package io.parapet.snapshot

/** Snapshotting / recovery configuration (see `dev-docs/snapshot.md`).
  *
  * @param enabled
  *   master switch. When on, the runtime periodically snapshots [[Snapshotable]] processes to `dataDir` as they run.
  * @param dataDir
  *   directory holding the per-process snapshot files; must survive restarts for recovery to be useful.
  * @param maxEventsPerSnapshot
  *   cadence ceiling: a process that keeps receiving events without draining its mailbox is snapshotted at least every
  *   this many deliveries. Bounds how many events a restore/replay has to re-fold.
  * @param maxSnapshotIntervalMillis
  *   time-based cadence for a continuously-busy process: while it has unsnapshotted state and never drains its mailbox,
  *   it is snapshotted at least this often (wall-clock). Bounds how much real-time work a crash can lose. `0` disables
  *   the time trigger, leaving cadence purely event-count driven.
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
