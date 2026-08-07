package io.parapet.core.journal

import com.typesafe.scalalogging.Logger
import io.parapet.effect.Monad.*
import io.parapet.effect.{Deferred, Effect, Retry}
import org.slf4j.LoggerFactory

import java.util.ArrayDeque
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Records deliveries to [[JournalStore]] with **sequenced admission and inline ordered publication** (v1).
  *
  * Sequence allocation and buffer insertion happen under one short lock, so admission order equals `seq` order and every
  * sealed batch is a consecutive `seq` slice - segment ranges are therefore strictly increasing and never overlap. The
  * lock never covers encoding, retries, sleeping, or filesystem IO.
  *
  * Publication is **inline**: an operation seals its batch and conditionally claims drain ownership under one lock, and
  * if it wins drives the store writes itself, one batch at a time in FIFO order; otherwise it awaits its batch. A
  * permanent write failure escapes through the operation and fails the application through the scheduler's own
  * supervision - there is no background writer whose failure must be polled.
  *
  * Each operation installs its [[Effect.guarantee]] finalizer *before* the locked seal-and-claim, so the finalizer
  * covers the state transition itself, not just the drain. If that finalizer still holds the ownership token, the owner
  * exited without settling publication (cancellation or an unexpected exit): the recorder **fails closed** - it
  * transitions to `Failed` and completes every pending batch - so no waiter is stranded, no phantom owner remains, and no
  * sealed-but-undrained batch is left behind a healthy `owner == null`. Interrupted publication is therefore terminal in
  * v1; recoverable cancellation would need an explicit owner-handoff protocol.
  *
  * v1 scope: count-, flush-, and close-triggered publication only - a count-bounded tail, no wall-clock timer, no byte
  * cap, no manifest.
  */
final class DeliveryRecorder[F[_]] private (
    store: JournalStore[F],
    config: JournalConfig,
    startSeq: Long
)(using effect: Effect[F]):

  import DeliveryRecorder.*

  private val logger       = Logger(LoggerFactory.getLogger(classOf[DeliveryRecorder[?]]))
  private val pollInterval = 1.milli

  // Everything below is guarded by `lock`. `owner` is the current drain-owner token, or null when no one is draining.
  private val lock            = new Object
  private var phase: Phase    = Phase.Open
  private var seq: Long       = startSeq
  private val active          = new Array[JournalEntry](config.batchSize)
  private var activeSize: Int = 0
  private val ready           = new ArrayDeque[SealedBatch[F]]()
  private var owner: AnyRef   = null

  /** Establishes the delivery's global position and admits it to the journal, returning its `seq`. A sealing admit
    * suspends until its batch is durable (bounded backpressure); it raises if the recorder has failed.
    */
  def admit(draft: JournalDraft): F[Long] =
    newSlot.flatMap { slot =>
      val token = new Object
      effect.guarantee(
        effect.delay(admitLocked(draft, slot, token)).flatMap {
          case AdmitOutcome.Rejected(error)           => effect.raiseError(error)
          case AdmitOutcome.Buffered(s)               => effect.pure(s)
          case AdmitOutcome.Sealed(s, batch, claimed) => driveThen(claimed, awaitBatch(batch)).as(s)
        }
      )(settleOwnerExit(token))
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

  /** Seals the current partial batch (if any) and publishes through the FIFO tail, so on return everything admitted
    * before this call is durable - including batches an earlier cancelled admit left in the FIFO.
    */
  def flush(): F[Unit] =
    newSlot.flatMap { slot =>
      val token = new Object
      effect.guarantee(
        effect.delay(flushLocked(slot, token)).flatMap {
          case FlushOutcome.Rejected(error)    => effect.raiseError(error)
          case FlushOutcome.Empty()            => effect.pure(())
          case FlushOutcome.Tail(batch, claimed) => driveThen(claimed, awaitBatch(batch))
        }
      )(settleOwnerExit(token))
    }

  /** Rejects new admissions, publishes everything already admitted, and marks the recorder closed. Fails with the
    * recorder's error if a write did not succeed, so a lost tail cannot be mistaken for a clean shutdown.
    */
  def close(): F[Unit] =
    newSlot.flatMap { slot =>
      val token = new Object
      effect.guarantee(
        effect.delay(closeLocked(slot, token)).flatMap {
          case CloseOutcome.AlreadyClosed     => effect.pure(())
          case CloseOutcome.Failed(error)     => effect.raiseError(error)
          case CloseOutcome.Proceed(claimed)  => driveThen(claimed, awaitDrained()) >> finishClose()
        }
      )(settleOwnerExit(token))
    }

  /** The highest `seq` durably recorded, or `None` when the journal is empty. */
  def maxSeq: F[Option[Long]] = store.maxSeq

  /** The highest envelope id the journal refers to, or `None` when the journal is empty. */
  def maxEnvelopeId: F[Option[Long]] = store.maxEnvelopeId

  private def newSlot: F[SealedSlot[F]] = Deferred[F, Either[Throwable, Unit]]()

  /** Drive the FIFO if this operation claimed ownership, then run its wait. */
  private def driveThen(claimed: Boolean, wait: F[Unit]): F[Unit] =
    (if claimed then drainLoop() else effect.pure(())) >> wait

  // ---- locked decisions (synchronous, executed inside effect.delay) ----

  private def admitLocked(draft: JournalDraft, slot: SealedSlot[F], token: AnyRef): AdmitOutcome[F] =
    lock.synchronized {
      phase match
        case Phase.Failed(error)          => AdmitOutcome.Rejected(error)
        case Phase.Closing | Phase.Closed => AdmitOutcome.Rejected(closedError)
        case Phase.Open =>
          seq += 1
          val s = seq
          active(activeSize) = draft.withSeq(s)
          activeSize += 1
          if activeSize >= config.batchSize then AdmitOutcome.Sealed(s, sealLocked(slot), claimLocked(token))
          else AdmitOutcome.Buffered(s)
    }

  private def flushLocked(slot: SealedSlot[F], token: AnyRef): FlushOutcome[F] =
    lock.synchronized {
      phase match
        case Phase.Failed(error) => FlushOutcome.Rejected(error)
        case Phase.Closed        => FlushOutcome.Rejected(closedError)
        case _ =>
          if activeSize > 0 then sealLocked(slot)
          if ready.isEmpty then FlushOutcome.Empty()
          else FlushOutcome.Tail(ready.peekLast(), claimLocked(token))
    }

  private def closeLocked(slot: SealedSlot[F], token: AnyRef): CloseOutcome =
    lock.synchronized {
      phase match
        case Phase.Closed        => CloseOutcome.AlreadyClosed
        case Phase.Failed(error) => CloseOutcome.Failed(error)
        case _ =>
          phase = Phase.Closing
          if activeSize > 0 then sealLocked(slot)
          CloseOutcome.Proceed(if ready.isEmpty then false else claimLocked(token))
    }

  /** Seals `active` into the FIFO. Caller must hold `lock`. */
  private def sealLocked(slot: SealedSlot[F]): SealedBatch[F] =
    val batch = SealedBatch(Vector.tabulate(activeSize)(active(_)), slot)
    activeSize = 0
    ready.addLast(batch)
    batch

  /** Claims drain ownership for `token` if unowned. Caller must hold `lock`. */
  private def claimLocked(token: AnyRef): Boolean =
    if owner == null then
      owner = token
      true
    else false

  // ---- publication (no lock held during store IO) ----

  /** Runs after the guaranteed body on every exit. Normal completion and store failure already released the token, so
    * this is a no-op then; if the token is still held the owner was aborted (cancellation), so fail closed.
    */
  private def settleOwnerExit(token: AnyRef): F[Unit] =
    effect.suspend {
      if lock.synchronized(token eq owner) then fail(new DrainOwnerAborted)
      else effect.pure(())
    }

  private def drainLoop(): F[Unit] =
    effect.delay(nextBatch()).flatMap {
      case None => effect.pure(())
      case Some(batch) =>
        store0(batch.entries)
          .flatMap { _ =>
            // Complete the waiter BEFORE removing the head. A batch's waiter is always signalled exactly one way: here
            // with Right (the store call above made it durable), or - if this drain is cut short first - by fail()
            // completing it with Left, which only reaches batches still in `ready`. Removing the head first would open a
            // cancellation window where the batch is neither completed nor still in `ready`, stranding its waiter.
            batch.completion.complete(Right(())).void >> effect.delay(completeHead()) >> drainLoop()
          }
          .handleErrorWith(error => fail(error) >> effect.raiseError(error))
    }

  private def nextBatch(): Option[SealedBatch[F]] =
    lock.synchronized {
      if ready.isEmpty then
        owner = null
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

  /** Transitions to `Failed` (first error wins) and completes every pending batch with that error so their operations
    * raise. Ownership and the FIFO are held until every completion finishes, so a cancellation mid-notification is
    * recoverable: the owner token stays installed, so [[settleOwnerExit]] re-runs this idempotent pass - preserving the
    * original error rather than replacing it - and only then clears the queue and ownership.
    */
  private def fail(error: Throwable): F[Unit] =
    effect.suspend {
      // Mark Failed and snapshot the waiting batches, but keep `ready` and `owner` intact: if the notification pass
      // below is cancelled part-way, the token is still installed, so settleOwnerExit re-enters and finishes it instead
      // of stranding the not-yet-completed waiters.
      val (effectiveError, pending) = lock.synchronized {
        val eff = phase match
          case Phase.Failed(existing) => existing
          case _                      => phase = Phase.Failed(error); error
        (eff, ready.iterator().asScala.toVector)
      }
      logger.error("journal recorder failed; failing every pending batch", effectiveError)
      pending.foldLeft(effect.pure(()))((acc, b) => acc >> b.completion.complete(Left(effectiveError)).void) >>
        effect.delay(lock.synchronized {
          ready.clear()
          owner = null
        })
    }

  private def awaitBatch(batch: SealedBatch[F]): F[Unit] =
    batch.completion.get.flatMap {
      case Right(_)    => effect.pure(())
      case Left(error) => effect.raiseError(error)
    }

  private def awaitDrained(): F[Unit] =
    effect.suspend {
      lock.synchronized((ready.isEmpty, owner == null, phase)) match
        case (_, _, Phase.Failed(error)) => effect.raiseError(error)
        case (true, true, _)             => effect.pure(())
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

  /** Signals that a drain owner exited without settling its publication (typically cancellation); the recorder fails
    * closed rather than strand waiters.
    */
  final class DrainOwnerAborted extends RuntimeException("journal drain owner aborted before settling publication")

  private type SealedSlot[F[_]] = Deferred[F, Either[Throwable, Unit]]

  private final case class SealedBatch[F[_]](entries: Vector[JournalEntry], completion: SealedSlot[F])

  private sealed trait Phase
  private object Phase:
    case object Open                          extends Phase
    case object Closing                       extends Phase
    case object Closed                        extends Phase
    final case class Failed(error: Throwable) extends Phase

  private sealed trait AdmitOutcome[F[_]]
  private object AdmitOutcome:
    final case class Rejected[F[_]](error: Throwable)                                    extends AdmitOutcome[F]
    final case class Buffered[F[_]](seq: Long)                                           extends AdmitOutcome[F]
    final case class Sealed[F[_]](seq: Long, batch: SealedBatch[F], claimed: Boolean)    extends AdmitOutcome[F]

  private sealed trait FlushOutcome[F[_]]
  private object FlushOutcome:
    final case class Rejected[F[_]](error: Throwable)                     extends FlushOutcome[F]
    final case class Empty[F[_]]()                                        extends FlushOutcome[F]
    final case class Tail[F[_]](batch: SealedBatch[F], claimed: Boolean)  extends FlushOutcome[F]

  private sealed trait CloseOutcome
  private object CloseOutcome:
    case object AlreadyClosed                  extends CloseOutcome
    final case class Failed(error: Throwable)  extends CloseOutcome
    final case class Proceed(claimed: Boolean) extends CloseOutcome

  private val closedError = new IllegalStateException("delivery recorder is closed")

  /** @param startSeq
    *   the delivery-sequence high-water to continue past, so a reopened journal neither reuses a `seq` nor overwrites a
    *   segment. The caller computes it as the maximum of the restored snapshot high-water, the journal's `maxSeq`, and
    *   (once it exists) the durable manifest - retained segments alone are not enough, since the journal may be fully
    *   truncated or the newest position may live only in a snapshot.
    */
  def apply[F[_]](store: JournalStore[F], config: JournalConfig = JournalConfig.default, startSeq: Long = 0L)(using
      Effect[F]
  ): DeliveryRecorder[F] =
    new DeliveryRecorder(store, config, startSeq)
