package io.parapet.core.journal

import com.typesafe.scalalogging.Logger
import io.parapet.core.Queue
import io.parapet.effect.Monad.*
import io.parapet.effect.{Deferred, Effect}
import org.slf4j.LoggerFactory

import java.util.PriorityQueue
import java.util.concurrent.atomic.AtomicBoolean

/** Buffers journal entries and flushes them to [[JournalStore]].
  *
  * [[append]] hands an entry to the worker and blocks if the writer is falling behind (backpressure); entries are
  * **never dropped** - losing one would make a delivery unrecoverable, unlike a dropped snapshot which only lengthens
  * replay. The worker accumulates entries in a per-`seq` priority queue and writes a batch once it reaches `batchSize`;
  * [[close]] flushes whatever remains and stops the worker.
  */
final class JournalManager[F[_]] private (
    store: JournalStore[F],
    queue: Queue[F, JournalManager.Item[F]],
    batchSize: Int
)(using effect: Effect[F]):

  import JournalManager.*

  private val logger = Logger(LoggerFactory.getLogger(classOf[JournalManager[?]]))
  private val closed = new AtomicBoolean(false)

  /** Appends `entry` to the journal. A no-op once [[close]] has run. */
  def append(entry: JournalEntry): F[Unit] =
    if closed.get() then effect.pure(())
    else queue.enqueue(Item.Store(entry))

  def close: F[Unit] =
    if !closed.compareAndSet(false, true) then effect.pure(())
    else Deferred[F, Unit]().flatMap(signal => queue.enqueue(Item.Flush(signal)).flatMap(_ => signal.get))

  private def drainLoop(buffer: PriorityQueue[JournalEntry]): F[Unit] =
    queue.dequeue.flatMap {
      case Item.Store(entry) =>
        effect
          .delay {
            buffer.add(entry)
            buffer.size() >= batchSize
          }
          .flatMap(full => (if full then flush(buffer) else effect.pure(())) >> drainLoop(buffer))
      case Item.Flush(signal) =>
        flush(buffer) >> signal.complete(()).void
    }

  /** Drains the buffer in ascending seq order and stores it as one batch; a store failure is logged, never fatal. */
  private def flush(buffer: PriorityQueue[JournalEntry]): F[Unit] =
    effect
      .delay {
        val batch = Vector.newBuilder[JournalEntry]
        while !buffer.isEmpty() do batch += buffer.poll()
        batch.result()
      }
      .flatMap { batch =>
        if batch.isEmpty then effect.pure(())
        else
          store
            .append(batch)
            .handleErrorWith(error =>
              effect.delay(logger.error(s"failed to store journal batch of ${batch.size} entries", error))
            )
      }

  private def startWorker(): F[Unit] =
    effect.start(drainLoop(newBuffer())).void

object JournalManager:

  /** Default number of entries buffered before a batch is flushed. */
  val DefaultBatchSize: Int = 1024

  /** Default background-writer queue capacity. */
  val DefaultQueueCapacity: Int = 4096

  private def newBuffer(): PriorityQueue[JournalEntry] =
    new PriorityQueue[JournalEntry](java.util.Comparator.comparingLong[JournalEntry](_.seq))

  /** An entry on the background writer's queue. */
  sealed private trait Item[F[_]]
  private object Item:
    final case class Store[F[_]](entry: JournalEntry)       extends Item[F]
    final case class Flush[F[_]](signal: Deferred[F, Unit]) extends Item[F]

  def apply[F[_]](
      store: JournalStore[F],
      batchSize: Int = DefaultBatchSize,
      queueCapacity: Int = DefaultQueueCapacity
  )(using effect: Effect[F]): F[JournalManager[F]] =
    Queue.bounded[F, Item[F]](queueCapacity, Queue.ChannelType.MPSC).flatMap { queue =>
      val manager = new JournalManager[F](store, queue, batchSize)
      manager.startWorker().as(manager)
    }
