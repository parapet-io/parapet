package io.parapet.core.journal

import com.typesafe.scalalogging.Logger
import io.parapet.core.Queue
import io.parapet.effect.Monad.*
import io.parapet.effect.{Effect, EffectFiber, Retry}
import org.slf4j.LoggerFactory

import java.util.PriorityQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.*

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
  private val workerRef  = new AtomicReference[EffectFiber[F, Unit]]()
  private val timerRef   = new AtomicReference[EffectFiber[F, Unit]]()

  /** The error that permanently failed the writer, if any. */
  def failure: Option[Throwable] = Option(failureRef.get())

  /** The highest seq durably recorded, or `None` when the journal is empty. */
  def maxSeq: F[Option[Long]] = store.maxSeq

  /** Completes when the writer stops: succeeds on a clean stop, fails with the error that stopped it. */
  def terminated: F[Unit] = joinWorker

  /** Appends `entry` to the journal. */
  def append(entry: JournalEntry): F[Unit] =
    effect.suspend {
      failureRef.get() match
        case null  => if closed.get() then effect.pure(()) else queue.enqueue(Item.Store(entry))
        case error => effect.raiseError(error)
    }

  /** Flushes pending entries and stops the writer. Fails with the writer's error if the final flush - or an earlier
    * write - did not succeed, so a lost journal tail cannot be mistaken for a clean shutdown.
    */
  def close: F[Unit] =
    effect.suspend {
      val firstClose = closed.compareAndSet(false, true)
      val stopTimer  = Option(timerRef.get()).fold(effect.pure(()))(_.cancel)
      val stopDrain  = if firstClose then signalStop else effect.pure(())
      stopTimer >> stopDrain >> joinWorker
    }

  /** Wakes the worker so it performs a final flush and exits. A live worker is draining and will free a slot, so we
    * retry; a failed worker never will, so we stop once its error is recorded and let [[joinWorker]] surface it.
    */
  private def signalStop: F[Unit] =
    queue.tryEnqueue(Item.Stop()).flatMap {
      case true  => effect.pure(())
      case false => if failureRef.get() != null then effect.pure(()) else effect.sleep(1.millis) >> signalStop
    }

  private def joinWorker: F[Unit] =
    effect.suspend {
      workerRef.get() match
        case null   => effect.pure(())
        case worker => worker.join
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
      case Item.Flush() =>
        flush(buffer) >> drainLoop(buffer)
      case Item.Stop() =>
        flush(buffer) // final flush; does not recurse, so the worker fiber completes here (or fails on flush error)
    }

  private def timerLoop: F[Unit] =
    effect.sleep(config.flushInterval).flatMap { _ =>
      if closed.get() then effect.pure(())
      else queue.tryEnqueue(Item.Flush()) >> timerLoop
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
    effect.start(drainLoop(newBuffer())).flatMap { worker =>
      effect.delay(workerRef.set(worker)) >>
        (if config.flushInterval > Duration.Zero then
           effect.start(timerLoop).flatMap(timer => effect.delay(timerRef.set(timer)))
         else effect.pure(()))
    }

object JournalManager:

  private def newBuffer(): PriorityQueue[JournalEntry] =
    new PriorityQueue[JournalEntry](java.util.Comparator.comparingLong[JournalEntry](_.seq))

  /** An entry on the background writer's queue. */
  sealed private trait Item[F[_]]
  private object Item:
    final case class Store[F[_]](entry: JournalEntry) extends Item[F]
    final case class Flush[F[_]]()                    extends Item[F]
    final case class Stop[F[_]]()                     extends Item[F]

  def apply[F[_]](store: JournalStore[F], config: JournalConfig = JournalConfig.default)(using
      effect: Effect[F]
  ): F[JournalManager[F]] =
    Queue.bounded[F, Item[F]](config.queueCapacity, Queue.ChannelType.MPSC).flatMap { queue =>
      val manager = new JournalManager[F](store, queue, config)
      manager.startWorker().as(manager)
    }
