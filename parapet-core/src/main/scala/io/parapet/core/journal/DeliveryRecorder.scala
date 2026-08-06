package io.parapet.core.journal

import com.typesafe.scalalogging.Logger
import io.parapet.effect.Monad.*
import io.parapet.effect.{Deferred, Effect, Retry}
import org.slf4j.LoggerFactory

import java.util.ArrayDeque
import scala.concurrent.duration.*

/** Records deliveries to [[JournalStore]] with **sequenced admission and inline ordered publication** (v1).
  *
  * Sequence allocation and buffer insertion happen under one short lock, so admission order equals `seq` order and every
  * sealed batch is a consecutive `seq` slice - segment ranges are therefore strictly increasing and never overlap. The
  * lock never covers encoding, retries, sleeping, or filesystem IO.
  *
  * Publication is **inline**: the caller whose `admit` seals a batch (or a `flush` / `close` caller) drives the store
  * writes itself, one batch at a time in FIFO order. A permanent write failure escapes that caller's `admit` and fails
  * the application through the scheduler's own supervision - there is no background writer whose failure must be polled.
  * The caller that seals a batch suspends until that batch is durable, which bounds in-flight batches to roughly the
  * worker count without an explicit queue.
  *
  * v1 scope: count-, flush-, and close-triggered publication only - a count-bounded tail, no wall-clock timer, no byte
  * cap, no manifest.
  */
final class DeliveryRecorder[F[_]] private (
    store: JournalStore[F],
    config: JournalConfig
)(using effect: Effect[F]):

  import DeliveryRecorder.*

  private val logger       = Logger(LoggerFactory.getLogger(classOf[DeliveryRecorder[?]]))
  private val pollInterval = 1.milli

  // Everything below is guarded by `lock`.
  private val lock              = new Object
  private var phase: Phase      = Phase.Open
  private var seq: Long         = 0L
  private val active            = new Array[JournalEntry](config.batchSize)
  private var activeSize: Int   = 0
  private val ready             = new ArrayDeque[SealedBatch[F]]()
  private var draining: Boolean = false

  /** Establishes the delivery's global position and admits it to the journal, returning its `seq`. When the admit fills
    * and seals a batch it suspends until that batch is durable (bounded backpressure); it raises if the recorder has
    * already failed.
    */
  def admit(draft: JournalDraft): F[Long] =
    newSlot.flatMap { slot =>
      effect.delay(admitLocked(draft, slot)).flatMap {
        case Outcome.Rejected(error)               => effect.raiseError(error)
        case Outcome.Buffered(s)                   => effect.pure(s)
        case Outcome.Sealed(s, becameOwner, batch) => publish(becameOwner, batch).as(s)
      }
    }

  /** Assigns a global position to a snapshot-tracked but unjournaled delivery. Shares the counter, so such deliveries
    * leave gaps in the journal's `seq` ranges but never cause overlap.
    */
  def sequenceOnly(): F[Long] =
    effect.suspend {
      lock.synchronized {
        phase match
          case Phase.Failed(error) => effect.raiseError(error)
          case _                   => seq += 1; effect.pure(seq)
      }
    }

  /** Seals and publishes the current partial batch, if any. */
  def flush(): F[Unit] =
    newSlot.flatMap { slot =>
      effect.delay(sealPartialLocked(slot)).flatMap {
        case Outcome.Rejected(error)               => effect.raiseError(error)
        case Outcome.Buffered(_)                   => effect.pure(())
        case Outcome.Sealed(_, becameOwner, batch) => publish(becameOwner, batch)
      }
    }

  /** Rejects new admissions, publishes everything already admitted, and marks the recorder closed. Fails with the
    * recorder's error if a write did not succeed, so a lost tail cannot be mistaken for a clean shutdown.
    */
  def close(): F[Unit] =
    newSlot.flatMap { slot =>
      effect.delay(beginCloseLocked(slot)).flatMap {
        case CloseBegin.AlreadyClosed        => effect.pure(())
        case CloseBegin.Failed(error)        => effect.raiseError(error)
        case CloseBegin.Proceed(becameOwner) =>
          (if becameOwner then drain() else effect.pure(())) >> awaitDrained() >> finishClose()
      }
    }

  /** The highest `seq` durably recorded, or `None` when the journal is empty. */
  def maxSeq: F[Option[Long]] = store.maxSeq

  /** The highest envelope id the journal refers to, or `None` when the journal is empty. */
  def maxEnvelopeId: F[Option[Long]] = store.maxEnvelopeId

  private def newSlot: F[SealedSlot[F]] = Deferred[F, Either[Throwable, Unit]]()

  /** After a seal: drive the FIFO if this caller owns it, then wait for its own batch to become durable. */
  private def publish(becameOwner: Boolean, batch: SealedBatch[F]): F[Unit] =
    (if becameOwner then drain() else effect.pure(())) >> awaitBatch(batch)

  // ---- locked decisions (synchronous, executed inside effect.delay/suspend) ----

  private def admitLocked(draft: JournalDraft, slot: SealedSlot[F]): Outcome[F] =
    lock.synchronized {
      phase match
        case Phase.Failed(error)          => Outcome.Rejected(error)
        case Phase.Closing | Phase.Closed => Outcome.Rejected(closedError)
        case Phase.Open =>
          seq += 1
          val s = seq
          active(activeSize) = draft.withSeq(s)
          activeSize += 1
          if activeSize >= config.batchSize then
            val (batch, becameOwner) = sealAndClaim(slot)
            Outcome.Sealed(s, becameOwner, batch)
          else Outcome.Buffered(s)
    }

  private def sealPartialLocked(slot: SealedSlot[F]): Outcome[F] =
    lock.synchronized {
      phase match
        case Phase.Failed(error) => Outcome.Rejected(error)
        case Phase.Closed        => Outcome.Rejected(closedError)
        case _ =>
          if activeSize == 0 then Outcome.Buffered(0L)
          else
            val (batch, becameOwner) = sealAndClaim(slot)
            Outcome.Sealed(0L, becameOwner, batch)
    }

  private def beginCloseLocked(slot: SealedSlot[F]): CloseBegin =
    lock.synchronized {
      phase match
        case Phase.Closed        => CloseBegin.AlreadyClosed
        case Phase.Failed(error) => CloseBegin.Failed(error)
        case _ =>
          phase = Phase.Closing
          if activeSize > 0 then CloseBegin.Proceed(sealAndClaim(slot)._2)
          else
            val becameOwner = !ready.isEmpty && !draining
            if becameOwner then draining = true
            CloseBegin.Proceed(becameOwner)
    }

  /** Seals `active` into the FIFO and claims drain ownership if no one holds it. Caller must hold `lock`. */
  private def sealAndClaim(slot: SealedSlot[F]): (SealedBatch[F], Boolean) =
    val batch = SealedBatch(Vector.tabulate(activeSize)(active(_)), slot)
    activeSize = 0
    ready.addLast(batch)
    val becameOwner = !draining
    if becameOwner then draining = true
    (batch, becameOwner)

  // ---- publication (no lock held during store IO) ----

  private def drain(): F[Unit] =
    effect.delay(nextBatch()).flatMap {
      case None => effect.pure(())
      case Some(batch) =>
        store0(batch.entries)
          .flatMap(_ => effect.delay(completeHead()) >> batch.completion.complete(Right(())).void >> drain())
          .handleErrorWith(error => fail(error) >> effect.raiseError(error))
    }

  private def nextBatch(): Option[SealedBatch[F]] =
    lock.synchronized {
      if ready.isEmpty then
        draining = false
        None
      else Some(ready.peekFirst()) // leave the head in place until the store acknowledges it
    }

  private def completeHead(): Unit =
    lock.synchronized {
      ready.removeFirst()
      ()
    }

  private def store0(entries: Vector[JournalEntry]): F[Unit] =
    Retry(
      config.maxRetries,
      config.backoff,
      (retry, error) =>
        logger.error(s"failed to store journal batch [${entries.head.seq}, ${entries.last.seq}] (retry $retry)", error)
    )(store.append(entries))

  /** Records the permanent failure, fails every waiting batch so their `admit`s raise, and releases ownership. */
  private def fail(error: Throwable): F[Unit] =
    effect
      .delay {
        lock.synchronized {
          phase = Phase.Failed(error)
          draining = false
          val pending = Vector.newBuilder[SealedBatch[F]]
          while !ready.isEmpty do pending += ready.removeFirst()
          pending.result()
        }
      }
      .flatMap { pending =>
        logger.error(s"journal recorder failed permanently; failing fast", error)
        pending.foldLeft(effect.pure(()))((acc, b) => acc >> b.completion.complete(Left(error)).void)
      }

  private def awaitBatch(batch: SealedBatch[F]): F[Unit] =
    batch.completion.get.flatMap {
      case Right(_)    => effect.pure(())
      case Left(error) => effect.raiseError(error)
    }

  private def awaitDrained(): F[Unit] =
    effect.suspend {
      lock.synchronized((ready.isEmpty, draining, phase)) match
        case (_, _, Phase.Failed(error)) => effect.raiseError(error)
        case (true, false, _)            => effect.pure(())
        case _                           => effect.sleep(pollInterval) >> awaitDrained()
    }

  private def finishClose(): F[Unit] =
    effect.delay {
      lock.synchronized {
        phase match
          case Phase.Failed(_) => ()
          case _               => phase = Phase.Closed
      }
    }

object DeliveryRecorder:

  private type SealedSlot[F[_]] = Deferred[F, Either[Throwable, Unit]]

  private final case class SealedBatch[F[_]](entries: Vector[JournalEntry], completion: SealedSlot[F])

  private sealed trait Phase
  private object Phase:
    case object Open                          extends Phase
    case object Closing                       extends Phase
    case object Closed                        extends Phase
    final case class Failed(error: Throwable) extends Phase

  private sealed trait Outcome[F[_]]
  private object Outcome:
    final case class Rejected[F[_]](error: Throwable)                                       extends Outcome[F]
    final case class Buffered[F[_]](seq: Long)                                              extends Outcome[F]
    final case class Sealed[F[_]](seq: Long, becameOwner: Boolean, batch: SealedBatch[F]) extends Outcome[F]

  private sealed trait CloseBegin
  private object CloseBegin:
    case object AlreadyClosed                     extends CloseBegin
    final case class Failed(error: Throwable)     extends CloseBegin
    final case class Proceed(becameOwner: Boolean) extends CloseBegin

  private val closedError = new IllegalStateException("delivery recorder is closed")

  def apply[F[_]](store: JournalStore[F], config: JournalConfig = JournalConfig.default)(using
      Effect[F]
  ): DeliveryRecorder[F] =
    new DeliveryRecorder(store, config)
