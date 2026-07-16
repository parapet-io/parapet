package io.parapet.core.snapshot

import io.parapet.ProcessRef
import io.parapet.core.Clock

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/** Runtime side of snapshot creation.
  *
  * Ids are **per process**: a snapshot is globally identified by `(processRef, id)`.
  *
  * Thread-safe; snapshots of different processes may be created concurrently.
  *
  * @param clock
  *   clock
  */
final class SnapshotManager(clock: Clock = Clock()):

  private val snapshotIdCounters = new ConcurrentHashMap[ProcessRef.Unknown, AtomicLong]()
  private val lineage            = new ConcurrentHashMap[ProcessRef.Unknown, java.lang.Long]()

  private def counterOf(ref: ProcessRef.Unknown): AtomicLong =
    snapshotIdCounters.computeIfAbsent(ref, _ => new AtomicLong(0L))

  /** Captures `process`'s current state as a new snapshot of `ref`.
    *
    * @param seq
    *   position of the last delivery `process` consumed; `0` when no delivery has been consumed yet.
    */
  def create(ref: ProcessRef.Unknown, process: Snapshotable, seq: Long): Snapshot =
    val id       = counterOf(ref).incrementAndGet()
    val parentId = Option(lineage.get(ref)).map(_.longValue()).getOrElse(0L)
    val snapshot = Snapshot(
      Metadata(
        processRef = ref,
        id = id,
        parentId = parentId,
        seq = seq,
        createdAt = clock.currentTimeMillis,
        schemaVersion = process.schemaVersion
      ),
      process.serialize()
    )
    lineage.put(ref, id)
    snapshot

  /** Restores `process` from `snapshot`.
    */
  def restore(process: Snapshotable, snapshot: Snapshot): Unit =
    process.restore(snapshot)
    val ref = snapshot.metadata.processRef
    lineage.put(ref, snapshot.metadata.id)
    counterOf(ref).set(snapshot.metadata.id)
    ()
