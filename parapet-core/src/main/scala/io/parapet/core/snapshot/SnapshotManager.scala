package io.parapet.core.snapshot

import com.typesafe.scalalogging.Logger
import io.parapet.ProcessRef
import io.parapet.core.{Clock, Queue}
import io.parapet.effect.Monad.*
import io.parapet.effect.{Deferred, Effect}
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

/** Manages snapshot creation and restoring process state; created snapshots are persisted to [[SnapshotStorage]].
  *
  * Two write paths:
  *   - [[create]] serializes and stores synchronously (returns the stored snapshot).
  *   - [[createAsync]] serializes synchronously then hands the snapshot to a single background worker that stores it.
  *
  * Ids continue across restarts: the first snapshot of a ref in a run is numbered after the latest one already in
  * storage. Snapshots of *different* processes may be created concurrently; for a *single* process, [[create]] /
  * [[createAsync]] must be called serially. A call whose `seq` is not strictly greater than that process's previous
  * snapshot fails - it signals a concurrent/out-of-order call, and therefore a torn read of the mutating state.
  */
final class SnapshotManager[F[_]] private (
    storage: SnapshotStorage[F],
    clock: Clock,
    queue: Queue[F, SnapshotManager.Item[F]]
)(using effect: Effect[F]):

  import SnapshotManager.*

  private val logger             = Logger(LoggerFactory.getLogger(classOf[SnapshotManager[?]]))
  private val snapshotIdCounters = new ConcurrentHashMap[ProcessRef.Unknown, AtomicLong]()
  private val lineage            = new ConcurrentHashMap[ProcessRef.Unknown, Long]()
  private val lastSeq            = new ConcurrentHashMap[ProcessRef.Unknown, java.lang.Long]()

  private val closed = new AtomicBoolean(false)

  /** Captures `process`'s current state as a new snapshot of `ref` and stores it synchronously.
    *
    * @param seq
    *   position of the last delivery `process` consumed; `0` when no delivery has been consumed yet.
    */
  def create(ref: ProcessRef.Unknown, process: Snapshotable, seq: Long): F[Snapshot] =
    build(ref, process, seq).flatMap(snapshot => storage.store(snapshot).as(snapshot))

  /** Captures `process`'s current state and enqueues it for the background worker to store. Returns as soon as it is
    * enqueued.
    *
    * Snapshots are best-effort: if the queue is full (the writer is falling behind), the snapshot is dropped with a
    * warning rather than blocking the caller.
    *
    * @return
    *   `true` if the snapshot was enqueued, `false` if it was dropped (queue full) or [[close]] has already run.
    */
  def createAsync(ref: ProcessRef.Unknown, process: Snapshotable, seq: Long): F[Boolean] =
    if closed.get() then effect.pure(false)
    else
      build(ref, process, seq).flatMap { snapshot =>
        queue.tryEnqueue(Item.Store(snapshot)).flatMap {
          case true  => effect.pure(true)
          case false =>
            effect
              .delay(logger.warn(s"snapshot queue full; dropping snapshot ${snapshot.metadata.id} of $ref"))
              .as(false)
        }
      }

  /** Stops accepting new snapshots, drains everything already enqueued, then stops the background worker; the returned
    * effect completes once all pending snapshots are on disk.
    *
    * A terminal marker is enqueued behind the pending snapshots. The worker stores each one in FIFO order, reaches the
    * marker, completes it, and exits - so completion means the backlog is flushed and the worker has stopped.
    */
  def close: F[Unit] =
    if !closed.compareAndSet(false, true) then effect.pure(())
    else Deferred[F, Unit]().flatMap(signal => queue.enqueue(Item.Flush(signal)).flatMap(_ => signal.get))

  /** Restores `process` from `snapshot`.
    *
    * A subsequent [[create]] continues the chain after every id already in storage - restoring an older snapshot (a
    * fork) does not reuse the ids of the abandoned newer ones.
    */
  def restore(process: Snapshotable, snapshot: Snapshot): F[Unit] =
    val ref = snapshot.metadata.processRef
    effect.delay {
      logger.debug("restoring snapshot=" + snapshot.metadata)
    } >>
      seededCounter(ref).map { _ =>
        process.restore(snapshot)
        lineage.put(ref, snapshot.metadata.id)
        ()
      }

  /** Restores `process` from its latest stored snapshot, if any, and returns that snapshot.
    */
  def restoreLatest(ref: ProcessRef.Unknown, process: Snapshotable): F[Option[Snapshot]] =
    storage.latest(ref).flatMap {
      case Some(snapshot) => restore(process, snapshot).as(Some(snapshot))
      case None           => effect.pure(None)
    }

  private def build(ref: ProcessRef.Unknown, process: Snapshotable, seq: Long): F[Snapshot] =
    seededCounter(ref).map { counter =>
      requireIncreasingSeq(ref, seq)
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
      lineage.put(ref, id)
      snapshot
    }

  /** Advances the ref's last snapshot seq to `seq`, requiring it to be strictly greater than the previous one.
    *
    * Guards the per-process serialization contract: successive snapshots of a ref must have a strictly greater seq. A
    * violation means [[create]] was called concurrently or out of order for the same process.
    */
  private def requireIncreasingSeq(ref: ProcessRef.Unknown, seq: Long): Unit =
    lastSeq.compute(
      ref,
      (_, previous) =>
        if previous == null || seq > previous.longValue() then seq
        else
          throw new IllegalStateException(
            s"out-of-order snapshot for $ref: seq $seq is not greater than the previous snapshot's seq $previous; " +
              s"create/createAsync must be called serially per process"
          )
    )
    ()

  /** The ref's id counter, advanced to the latest id in storage on first use in this run. */
  private def seededCounter(ref: ProcessRef.Unknown): F[AtomicLong] =
    val counter = snapshotIdCounters.computeIfAbsent(ref, _ => new AtomicLong(0L))
    if counter.get() != 0L then effect.pure(counter)
    else
      storage.latest(ref).map { latest =>
        latest.foreach(snapshot => counter.updateAndGet(current => math.max(current, snapshot.metadata.id)))
        counter
      }

  /** Stores queued snapshots in enqueue order; a store failure is logged, never fatal. The terminal [[Item.Flush]]
    * marker completes its signal and ends the loop.
    */
  private def drainLoop: F[Unit] =
    queue.dequeue.flatMap {
      case Item.Store(snapshot) =>
        storage
          .store(snapshot)
          .handleErrorWith(error =>
            effect.delay(logger.error(s"failed to store snapshot ${snapshot.metadata.id}", error))
          )
          .flatMap(_ => drainLoop)
      case Item.Flush(signal) =>
        signal.complete(()).void
    }

  private def startWorker(): F[Unit] =
    effect.start(drainLoop).void

object SnapshotManager:

  /** Default background-writer queue capacity. */
  val DefaultQueueCapacity: Int = 1024

  /** An entry on the background writer's queue. */
  sealed private trait Item[F[_]]
  private object Item:
    final case class Store[F[_]](snapshot: Snapshot)        extends Item[F]
    final case class Flush[F[_]](signal: Deferred[F, Unit]) extends Item[F]

  def apply[F[_]](
      storage: SnapshotStorage[F],
      clock: Clock = Clock(),
      queueCapacity: Int = DefaultQueueCapacity
  )(using effect: Effect[F]): F[SnapshotManager[F]] =
    Queue.bounded[F, Item[F]](queueCapacity, Queue.ChannelType.MPSC).flatMap { queue =>
      val manager = new SnapshotManager[F](storage, clock, queue)
      manager.startWorker().as(manager)
    }
