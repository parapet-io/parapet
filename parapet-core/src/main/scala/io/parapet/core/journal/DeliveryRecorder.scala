package io.parapet.core.journal

import com.typesafe.scalalogging.Logger
import io.parapet.effect.Monad.*
import io.parapet.effect.{Deferred, Effect, Retry}
import org.slf4j.LoggerFactory

import java.util.ArrayDeque
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Records deliveries to a [[JournalStore]], assigning each a global delivery `seq`.
  *
  * Admission is totally ordered: `seq` is assigned in the same step that buffers the entry, so admission order equals
  * `seq` order and every batch written to the store is a consecutive `seq` slice. Segment ranges are therefore strictly
  * increasing and never overlap.
  *
  * An [[admit]] that fills a batch, and [[flush]] and [[close]], return only once the affected batch is durable, so a
  * caller waiting on a batch backpressures on the store's write rate. A permanent write failure is terminal: it fails
  * the recorder, so every in-flight and subsequent operation raises. A publication interrupted by cancellation is also
  * terminal.
  *
  * v1 scope: batches are published on reaching `batchSize`, on [[flush]], and on [[close]] - there is no wall-clock
  * timer, so the un-published tail is bounded by count, not time; no byte cap and no durable manifest yet.
  */
final class DeliveryRecorder[F[_]] private (
    store: JournalStore[F],
    config: JournalConfig,
    startSeq: Long
)(using effect: Effect[F]):

  import DeliveryRecorder.*

  private val logger       = Logger(LoggerFactory.getLogger(classOf[DeliveryRecorder[?]]))
  private val pollInterval = 1.milli

  // Implementation:
  //
  // Sequence allocation and buffer insertion happen under `lock` in one step (so admission order == seq order); the lock
  // never covers encoding, retries, sleeping, or filesystem IO. Publication is inline: a sealing operation conditionally
  // claims drain ownership - an identity token stored in `owner` - and if it wins drives the store writes itself, one
  // batch at a time in FIFO order; a non-owner instead awaits its batch's completion.
  //
  // Ownership is claimed *inside* an `effect.guarantee` scope whose finalizer (`settleOwnerExit`) releases it, so a
  // cancelled or otherwise abnormally-exited owner cannot leave the FIFO owned-but-idle. If that finalizer still holds
  // the token, the drain did not settle, so `fail` runs - transition to Failed and complete every pending batch - which
  // is why interrupted publication is terminal. The identity token (rather than a boolean) prevents a stale finalizer
  // from releasing a newer owner.
  //
  // Owner fairness (v1 compromise, not addressed): the owner drains until the FIFO is empty, not just until its own
  // batch is durable, so under continuous traffic one caller can remain the de-facto writer, and an owning `flush` can
  // end up waiting for batches admitted after it. Acceptable for v1; revisit with a bounded drain quantum or an
  // ownership handoff if it shows up as latency unfairness.
  //
  // Everything below is guarded by `lock`. `owner` is the current drain-owner token, or null when no one is draining.
  private val lock            = new Object
  private var phase: Phase    = Phase.Open
  private var seq: Long       = startSeq
  private var active          = new Array[JournalEntry](config.batchSize)
  private var activeSize: Int = 0
  private val ready           = new ArrayDeque[SealedBatch[F]]()
  private var owner: AnyRef   = null

  /** Establishes the delivery's global position and admits it to the journal, returning its `seq`. A sealing admit
    * suspends until its batch is durable (bounded backpressure); it raises if the recorder has failed.
    */
  def admit(draft: JournalDraft): F[Long] =
    effect.suspend {
      val token = new Object
      effect.guarantee(
        effect.delay(admitLocked(draft, token)).flatMap {
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
          case Phase.Failed(error)          => effect.raiseError(error)
          case Phase.Closing | Phase.Closed => effect.raiseError(closedError)
          case Phase.Open                   =>
            if seq == Long.MaxValue then effect.raiseError(seqExhausted)
            else
              seq += 1
              effect.pure(seq)
      }
    }

  /** Seals the current partial batch (if any) and publishes through the FIFO tail, so on return everything admitted
    * before this call is durable - including batches an earlier cancelled admit left in the FIFO.
    */
  def flush(): F[Unit] =
    effect.suspend {
      val token = new Object
      effect.guarantee(
        effect.delay(flushLocked(token)).flatMap {
          case FlushOutcome.Rejected(error)      => effect.raiseError(error)
          case FlushOutcome.Empty()              => effect.pure(())
          case FlushOutcome.Tail(batch, claimed) => driveThen(claimed, awaitBatch(batch))
        }
      )(settleOwnerExit(token))
    }

  /** Rejects new admissions, publishes everything already admitted, and marks the recorder closed. Fails with the
    * recorder's error if a write did not succeed, so a lost tail cannot be mistaken for a clean shutdown.
    */
  def close(): F[Unit] =
    effect.suspend {
      val token = new Object
      effect.guarantee(
        effect.delay(closeLocked(token)).flatMap {
          case CloseOutcome.AlreadyClosed    => effect.pure(())
          case CloseOutcome.Failed(error)    => effect.raiseError(error)
          case CloseOutcome.Proceed(claimed) => driveThen(claimed, awaitDrained()) >> finishClose()
        }
      )(settleOwnerExit(token))
    }

  /** The highest `seq` durably recorded, or `None` when the journal is empty. */
  def maxSeq: F[Option[Long]] = store.maxSeq

  /** The highest envelope id the journal refers to, or `None` when the journal is empty. */
  def maxEnvelopeId: F[Option[Long]] = store.maxEnvelopeId

  /** Drive the FIFO if this operation claimed ownership, then run its wait. */
  private def driveThen(claimed: Boolean, wait: F[Unit]): F[Unit] =
    (if claimed then drainLoop() else effect.pure(())) >> wait

  // ---- locked decisions (synchronous, executed inside effect.delay) ----

  private def admitLocked(draft: JournalDraft, token: AnyRef): AdmitOutcome[F] =
    lock.synchronized {
      phase match
        case Phase.Failed(error)          => AdmitOutcome.Rejected(error)
        case Phase.Closing | Phase.Closed => AdmitOutcome.Rejected(closedError)
        case Phase.Open                   =>
          if seq == Long.MaxValue then AdmitOutcome.Rejected(seqExhausted)
          else
            seq += 1
            val s = seq
            active(activeSize) = draft.withSeq(s)
            activeSize += 1
            if activeSize >= config.batchSize then AdmitOutcome.Sealed(s, sealLocked(), claimLocked(token))
            else AdmitOutcome.Buffered(s)
    }

  private def flushLocked(token: AnyRef): FlushOutcome[F] =
    lock.synchronized {
      phase match
        case Phase.Failed(error) => FlushOutcome.Rejected(error)
        case Phase.Closed        => FlushOutcome.Rejected(closedError)
        case _                   =>
          if activeSize > 0 then sealLocked()
          if ready.isEmpty then FlushOutcome.Empty()
          else FlushOutcome.Tail(ready.peekLast(), claimLocked(token))
    }

  private def closeLocked(token: AnyRef): CloseOutcome =
    lock.synchronized {
      phase match
        case Phase.Closed        => CloseOutcome.AlreadyClosed
        case Phase.Failed(error) => CloseOutcome.Failed(error)
        case _                   =>
          phase = Phase.Closing
          if activeSize > 0 then sealLocked()
          CloseOutcome.Proceed(if ready.isEmpty then false else claimLocked(token))
    }

  /** Seals `active` by swapping in a fresh buffer in O(1) and handing the filled one (plus its length) to the batch;
    * the copy into the durable form happens outside the lock in [[store0]]. Caller must hold `lock`.
    */
  private def sealLocked(): SealedBatch[F] =
    val batch = SealedBatch(active, activeSize, Deferred.unsafe[F, Either[Throwable, Unit]]())
    active = new Array[JournalEntry](config.batchSize)
    activeSize = 0
    ready.addLast(batch)
    batch

  /** Releases the buffered (not-yet-sealed) entries so a terminal recorder doesn't pin them. Caller must hold `lock`. */
  private def clearActiveLocked(): Unit =
    var i = 0
    while i < activeSize do
      active(i) = null
      i += 1
    activeSize = 0

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

  /** Drives the FIFO to empty (or to the first failure). One error handler wraps the whole drain, so a failure invokes
    * [[fail]] once - not once per already-drained batch - and no per-batch handler frame accumulates as the loop runs.
    */
  private def drainLoop(): F[Unit] =
    drainBatches().handleErrorWith(error => fail(error) >> effect.raiseError(error))

  private def drainBatches(): F[Unit] =
    effect.delay(nextBatch()).flatMap {
      case None        => effect.pure(())
      case Some(batch) =>
        store0(batch).flatMap { _ =>
          // Complete the waiter BEFORE removing the head. A batch's waiter is always signalled exactly one way: here
          // with Right (the store call above made it durable), or - if this drain is cut short first - by fail()
          // completing it with Left, which only reaches batches still in `ready`. Removing the head first would open a
          // cancellation window where the batch is neither completed nor still in `ready`, stranding its waiter.
          batch.completion.complete(Right(())).void >> effect.delay(completeHead()) >> drainBatches()
        }
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

  private def store0(batch: SealedBatch[F]): F[Unit] =
    // Copy the sealed buffer into its durable form here, off the admission lock. The buffer is never mutated after
    // sealing (admits write the fresh `active`), so this read is safe without holding the lock.
    effect.delay(Vector.tabulate(batch.size)(batch.entries(_))).flatMap { entries =>
      Retry(
        config.maxRetries,
        config.backoff,
        (retry, error) =>
          logger.error(s"failed to store journal batch [${entries.head.seq}, ${entries.last.seq}] (retry $retry)", error)
      )(store.append(entries))
    }

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
          case _ =>
            phase = Phase.Failed(error)
            clearActiveLocked() // the buffered partial batch will never be sealed now; release it (no waiter awaits it)
            error
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

  final private case class SealedBatch[F[_]](entries: Array[JournalEntry], size: Int, completion: SealedSlot[F])

  sealed private trait Phase
  private object Phase:
    case object Open                          extends Phase
    case object Closing                       extends Phase
    case object Closed                        extends Phase
    final case class Failed(error: Throwable) extends Phase

  sealed private trait AdmitOutcome[F[_]]
  private object AdmitOutcome:
    final case class Rejected[F[_]](error: Throwable)                                 extends AdmitOutcome[F]
    final case class Buffered[F[_]](seq: Long)                                        extends AdmitOutcome[F]
    final case class Sealed[F[_]](seq: Long, batch: SealedBatch[F], claimed: Boolean) extends AdmitOutcome[F]

  sealed private trait FlushOutcome[F[_]]
  private object FlushOutcome:
    final case class Rejected[F[_]](error: Throwable)                    extends FlushOutcome[F]
    final case class Empty[F[_]]()                                       extends FlushOutcome[F]
    final case class Tail[F[_]](batch: SealedBatch[F], claimed: Boolean) extends FlushOutcome[F]

  sealed private trait CloseOutcome
  private object CloseOutcome:
    case object AlreadyClosed                  extends CloseOutcome
    final case class Failed(error: Throwable)  extends CloseOutcome
    final case class Proceed(claimed: Boolean) extends CloseOutcome

  private val closedError  = new IllegalStateException("delivery recorder is closed")
  private val seqExhausted = new IllegalStateException("journal delivery sequence exhausted at Long.MaxValue")

  /** @param startSeq
    *   the delivery-sequence high-water to continue past, so a reopened journal neither reuses a `seq` nor overwrites a
    *   segment. The caller computes it as the maximum of the restored snapshot high-water, the journal's `maxSeq`, and
    *   (once it exists) the durable manifest - retained segments alone are not enough, since the journal may be fully
    *   truncated or the newest position may live only in a snapshot.
    */
  def apply[F[_]](store: JournalStore[F], config: JournalConfig = JournalConfig.default, startSeq: Long = 0L)(using
      Effect[F]
  ): DeliveryRecorder[F] =
    require(config.batchSize > 0, s"journal batchSize must be > 0, got ${config.batchSize}")
    require(config.maxRetries >= 0, s"journal maxRetries must be >= 0, got ${config.maxRetries}")
    require(startSeq >= 0, s"journal startSeq must be >= 0, got $startSeq")
    new DeliveryRecorder(store, config, startSeq)
