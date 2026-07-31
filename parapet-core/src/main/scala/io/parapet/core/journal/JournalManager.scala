package io.parapet.core.journal

import com.typesafe.scalalogging.Logger
import io.parapet.core.Queue
import io.parapet.effect.Monad.*
import io.parapet.effect.{Deferred, Effect, Retry}
import org.slf4j.LoggerFactory

import java.util.PriorityQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

/** Buffers journal entries and writes them to [[JournalStore]] in seq-sorted batches on a background worker. */
final class JournalManager[F[_]] private (
    store: JournalStore[F],
    queue: Queue[F, JournalManager.Item[F]],
    config: JournalConfig
)(using effect: Effect[F]):

  import JournalManager.*

  private val logger     = Logger(LoggerFactory.getLogger(classOf[JournalManager[?]]))
  private val closed     = new AtomicBoolean(false)
  private val failureRef = new AtomicReference[Throwable]()

  /** The error that permanently failed the writer, if any. */
  def failure: Option[Throwable] = Option(failureRef.get())

  /** Appends `entry` to the journal. */
  def append(entry: JournalEntry): F[Unit] =
    effect.suspend {
      failureRef.get() match
        case null  => if closed.get() then effect.pure(()) else queue.enqueue(Item.Store(entry))
        case error => effect.raiseError(error)
    }

  /** Flushes pending entries and stops the writer. */
  def close: F[Unit] =
    effect.suspend {
      if !closed.compareAndSet(false, true) || failureRef.get() != null then effect.pure(())
      else Deferred[F, Unit]().flatMap(signal => queue.enqueue(Item.Flush(signal)).flatMap(_ => signal.get))
    }

  private def drainLoop(buffer: PriorityQueue[JournalEntry]): F[Unit] =
    queue.dequeue.flatMap {
      case Item.Store(entry) =>
        effect
          .delay {
            buffer.add(entry)
            buffer.size() >= config.batchSize
          }
          .flatMap(full => (if full then flush(buffer) else effect.pure(())) >> drainLoop(buffer))
      case Item.Flush(signal) =>
        effect.guarantee(flush(buffer))(signal.complete(()).void)
    }

  private def flush(buffer: PriorityQueue[JournalEntry]): F[Unit] =
    effect
      .delay {
        val batch = Vector.newBuilder[JournalEntry]
        while !buffer.isEmpty() do batch += buffer.poll()
        batch.result()
      }
      .flatMap(batch => if batch.isEmpty then effect.pure(()) else store0(batch))

  private def store0(batch: Vector[JournalEntry]): F[Unit] =
    Retry(
      config.maxRetries,
      config.backoff,
      (retry, error) => logger.error(s"failed to store journal batch of ${batch.size} entries (retry $retry)", error)
    )(store.append(batch))
      .handleErrorWith { error =>
        effect.delay {
          failureRef.compareAndSet(null, error)
          logger.error(s"journal writer failed permanently after ${config.maxRetries} retries; failing fast", error)
        } >> effect.raiseError(error)
      }

  private def startWorker(): F[Unit] =
    effect.start(drainLoop(newBuffer())).void

object JournalManager:

  private def newBuffer(): PriorityQueue[JournalEntry] =
    new PriorityQueue[JournalEntry](java.util.Comparator.comparingLong[JournalEntry](_.seq))

  /** An entry on the background writer's queue. */
  sealed private trait Item[F[_]]
  private object Item:
    final case class Store[F[_]](entry: JournalEntry)       extends Item[F]
    final case class Flush[F[_]](signal: Deferred[F, Unit]) extends Item[F]

  def apply[F[_]](store: JournalStore[F], config: JournalConfig = JournalConfig.default)(using
      effect: Effect[F]
  ): F[JournalManager[F]] =
    Queue.bounded[F, Item[F]](config.queueCapacity, Queue.ChannelType.MPSC).flatMap { queue =>
      val manager = new JournalManager[F](store, queue, config)
      manager.startWorker().as(manager)
    }
