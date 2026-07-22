package io.parapet.core.snapshot

import io.parapet.ProcessRef
import io.parapet.core.Clock
import io.parapet.effect.Effect

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/** Manages snapshot creation and restoring process state; created snapshots are persisted to [[SnapshotStorage]].
  *
  * Ids continue across restarts: the first snapshot of a ref in a run is numbered after the newest one already in
  * storage.
  *
  * Thread-safe; snapshots of different processes may be created concurrently.
  *
  * @param storage
  *   where snapshots are persisted and looked up.
  * @param clock
  *   clock
  */
final class SnapshotManager[F[_]](storage: SnapshotStorage[F], clock: Clock = Clock())(using effect: Effect[F]):

  private val snapshotIdCounters = new ConcurrentHashMap[ProcessRef.Unknown, AtomicLong]()
  private val lineage            = new ConcurrentHashMap[ProcessRef.Unknown, Long]()

  /** Captures `process`'s current state as a new snapshot of `ref` and persists it.
    *
    * @param seq
    *   position of the last delivery `process` consumed; `0` when no delivery has been consumed yet.
    */
  def create(ref: ProcessRef.Unknown, process: Snapshotable, seq: Long): F[Snapshot] =
    seededCounter(ref).flatMap { counter =>
      effect.suspend {
        val id       = counter.incrementAndGet()
        val parentId = Option(lineage.get(ref)).map(_.longValue()).getOrElse(0L)
        val snapshot = Snapshot(
          Snapshot.Metadata(
            processRef = ref,
            id = id,
            parentId = parentId,
            seq = seq,
            createdAt = clock.currentTimeMillis,
            schemaVersion = process.schemaVersion
          ),
          process.serialize()
        )
        storage.store(snapshot).map { _ =>
          lineage.put(ref, id)
          snapshot
        }
      }
    }

  /** Restores `process` from `snapshot`.
    *
    * A subsequent [[create]] continues the chain after every id already in storage - restoring an older snapshot (a
    * fork) does not reuse the ids of the abandoned newer ones.
    */
  def restore(process: Snapshotable, snapshot: Snapshot): F[Unit] =
    val ref = snapshot.metadata.processRef
    seededCounter(ref).map { _ =>
      process.restore(snapshot)
      lineage.put(ref, snapshot.metadata.id)
      ()
    }

  /** The ref's id counter, advanced to the newest id in storage on first use in this run. */
  private def seededCounter(ref: ProcessRef.Unknown): F[AtomicLong] =
    val counter = snapshotIdCounters.computeIfAbsent(ref, _ => new AtomicLong(0L))
    if counter.get() != 0L then effect.pure(counter)
    else
      storage.latest(ref).map { newest =>
        newest.foreach(snapshot => counter.updateAndGet(current => math.max(current, snapshot.metadata.id)))
        counter
      }
