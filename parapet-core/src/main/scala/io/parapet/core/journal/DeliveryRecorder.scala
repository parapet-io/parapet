package io.parapet.core.journal

import com.typesafe.scalalogging.Logger
import io.parapet.core.Clock
import io.parapet.effect.Monad.*
import io.parapet.effect.{Deferred, Effect, Retry}
import org.slf4j.LoggerFactory

import java.util.ArrayDeque
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success}

/** Records deliveries to a [[JournalStore]], assigning each a global delivery `seq`.
  *
  * Admission is totally ordered: `seq` is assigned in the same step that buffers the entry, so admission order equals
  * `seq` order. Entries within a batch and batches within the journal are therefore strictly increasing, and batch
  * ranges never overlap. Calls to [[advanceSequence]] may leave gaps between journaled positions.
  *
  * An [[admit]] that fills a batch, and [[flush]] and [[close]], return only once the affected batch is durable, so a
  * caller waiting on a batch backpressures on the store's write rate. A permanent write failure is terminal: it fails
  * the recorder, so every in-flight and subsequent operation raises. A publication interrupted by cancellation is also
  * terminal.
  *
  * A publication owner drains the FIFO before returning. Under continuous concurrent admission this can delay that
  * caller beyond the durability of its own batch; a finite workload drains, and [[close]] fences admission before it
  * waits. Batches are published on reaching `batchSize`, on [[flush]], and on [[close]]. There is no wall-clock trigger
  * or byte limit, so an unpublished tail is bounded by entry count rather than age or encoded size.
  */
final class DeliveryRecorder[F[_]] private (
    store: JournalStore[F],
    config: JournalConfig,
    startSeq: Long,
    registry: EventCodecRegistry,
    clock: Clock
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
        encode(draft).flatMap { encoded =>
          effect.delay(admit(encoded, draft, token)).flatMap {
            case AdmitOutcome.Rejected(error)           => effect.raiseError(error)
            case AdmitOutcome.Buffered(s)               => effect.pure(s)
            case AdmitOutcome.Sealed(s, batch, claimed) => (drainIfOwner(claimed) >> awaitBatch(batch)).as(s)
          }
        }
      )(settleOwnerExit(token))
    }

  /** Encodes the draft's event off the admission lock. The codec is expected present (the caller admits only
    * journalable events); a missing codec or an encode failure is a fault and raises.
    */
  private def encode(draft: JournalDraft): F[Encoded] =
    effect.suspend {
      registry.codecFor(draft.event) match
        case None =>
          effect.raiseError(new IllegalStateException(s"no journal codec for event ${draft.event.getClass.getName}"))
        case Some(codec) =>
          codec.encode(draft.event) match
            case Success(bytes) => effect.pure(Encoded(bytes.clone(), codec.tag, codec.version))
            case Failure(error) => effect.raiseError(error)
    }

  def advanceSequence(): F[Long] =
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
        effect.delay(flush(token)).flatMap {
          case FlushOutcome.Rejected(error)      => effect.raiseError(error)
          case FlushOutcome.Empty()              => effect.pure(())
          case FlushOutcome.Tail(batch, claimed) => drainIfOwner(claimed) >> awaitBatch(batch)
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
        effect.delay(close(token)).flatMap {
          case CloseOutcome.AlreadyClosed    => effect.pure(())
          case CloseOutcome.Failed(error)    => effect.raiseError(error)
          case CloseOutcome.Proceed(claimed) => drainIfOwner(claimed) >> awaitDrained() >> finishClose()
        }
      )(settleOwnerExit(token))
    }

  /** Advances the delivery sequence past `after`. For boot seeding only - must be called before any admission.
    */
  def continueAfter(after: Long): Unit =
    lock.synchronized { if after > seq then seq = after }

  /** The highest `seq` durably recorded, or `None` when the journal is empty. */
  def maxSeq: F[Option[Long]] = store.maxSeq

  /** The highest envelope id the journal refers to, or `None` when the journal is empty. */
  def maxEnvelopeId: F[Option[Long]] = store.maxEnvelopeId

  /** Drives the FIFO if this operation claimed ownership; a no-op otherwise. */
  private def drainIfOwner(claimed: Boolean): F[Unit] =
    if claimed then drainLoop() else effect.pure(())

  private def admit(encoded: Encoded, draft: JournalDraft, token: AnyRef): AdmitOutcome[F] =
    lock.synchronized {
      phase match
        case Phase.Failed(error)          => AdmitOutcome.Rejected(error)
        case Phase.Closing | Phase.Closed => AdmitOutcome.Rejected(closedError)
        case Phase.Open                   =>
          if seq == Long.MaxValue then AdmitOutcome.Rejected(seqExhausted)
          else
            seq += 1
            val s = seq
            active(activeSize) = JournalEntry(
              s,
              draft.id,
              draft.sender,
              draft.receiver,
              draft.cause,
              encoded.bytes,
              encoded.tag,
              encoded.version
            )
            activeSize += 1
            if activeSize >= config.batchSize then AdmitOutcome.Sealed(s, sealLocked(), claimLocked(token))
            else AdmitOutcome.Buffered(s)
    }

  private def flush(token: AnyRef): FlushOutcome[F] =
    lock.synchronized {
      phase match
        case Phase.Failed(error) => FlushOutcome.Rejected(error)
        case Phase.Closed        => FlushOutcome.Rejected(closedError)
        case _                   =>
          if activeSize > 0 then sealLocked()
          if ready.isEmpty then FlushOutcome.Empty()
          else FlushOutcome.Tail(ready.peekLast(), claimLocked(token))
    }

  private def close(token: AnyRef): CloseOutcome =
    lock.synchronized {
      phase match
        case Phase.Closed        => CloseOutcome.AlreadyClosed
        case Phase.Failed(error) => CloseOutcome.Failed(error)
        case _                   =>
          phase = Phase.Closing
          if activeSize > 0 then sealLocked()
          CloseOutcome.Proceed(if ready.isEmpty then false else claimLocked(token))
    }

  // Hands the sealed buffer (plus its length) to the batch and installs a fresh active buffer. Conversion to the
  // durable Vector happens outside the lock in store0. Caller must hold `lock`.
  private def sealLocked(): SealedBatch[F] =
    val batch = SealedBatch(active, activeSize, Deferred.unsafe[F, Either[Throwable, Unit]]())
    active = new Array[JournalEntry](config.batchSize)
    activeSize = 0
    ready.addLast(batch)
    batch

  /** Releases the buffered (not-yet-sealed) entries so a terminal recorder doesn't pin them. Caller must hold `lock`.
    */
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
      case Some(batch) => store0(batch) >> completeHead(batch) >> drainBatches()
    }

  private def nextBatch(): Option[SealedBatch[F]] =
    lock.synchronized {
      // advances the drain: the FIFO head to write next, or - atomically with observing the empty FIFO - releases
      // ownership and returns `None`. An empty FIFO *is* the end of the drain, so the check and the release must be one
      // lock acquisition: split them and a concurrent `admit` could enqueue a batch (seeing ownership still held, so not
      // claiming) in the gap, then this releases - leaving a batch in `ready` with no owner to drain it.
      if ready.isEmpty then
        owner = null
        None
      else Some(ready.peekFirst()) // leave the head in place until the store acknowledges it
    }

  private def completeHead(batch: SealedBatch[F]): F[Unit] =
    effect.delay {
      lock.synchronized {
        batch.completion.unsafeComplete(Right(()))
        ready.removeFirst()
        ()
      }
    }

  private def store0(batch: SealedBatch[F]): F[Unit] =
    // Build the durable segment here, off the admission lock. The buffer is never mutated after sealing (admits write
    // the fresh `active`), so this read is safe without holding the lock.
    effect
      .delay {
        val entries = Vector.tabulate(batch.size)(batch.entries(_))
        JournalSegment(JournalMetadata.of(entries, clock.currentTimeMillis), entries)
      }
      .flatMap { segment =>
        val m = segment.metadata
        Retry(
          config.maxRetries,
          config.backoff,
          (retry, error) =>
            logger.error(s"failed to store journal segment [${m.minSeq}, ${m.maxSeq}] (retry $retry)", error)
        )(store.append(segment))
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
          case _                      =>
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

  /** An event encoded for the journal: its payload bytes plus the codec identity that produced them. */
  final private case class Encoded(bytes: Array[Byte], tag: String, version: Int)

  /** Creates a recorder for a journal with no previously assigned delivery positions. */
  def fresh[F[_]](
      store: JournalStore[F],
      registry: EventCodecRegistry = EventCodecRegistry.empty,
      config: JournalConfig = JournalConfig.default,
      clock: Clock = Clock()
  )(using Effect[F]): DeliveryRecorder[F] =
    create(store, config, 0L, registry, clock)

  /** Creates a recorder that continues after `highWater`.
    *
    * @param highWater
    *   the highest delivery position already represented by recovered durable state.
    */
  def resume[F[_]](
      store: JournalStore[F],
      highWater: Long,
      registry: EventCodecRegistry = EventCodecRegistry.empty,
      config: JournalConfig = JournalConfig.default,
      clock: Clock = Clock()
  )(using Effect[F]): DeliveryRecorder[F] =
    create(store, config, highWater, registry, clock)

  private def create[F[_]](
      store: JournalStore[F],
      config: JournalConfig,
      highWater: Long,
      registry: EventCodecRegistry,
      clock: Clock
  )(using Effect[F]): DeliveryRecorder[F] =
    require(config.batchSize > 0, s"journal batchSize must be > 0, got ${config.batchSize}")
    require(config.maxRetries >= 0, s"journal maxRetries must be >= 0, got ${config.maxRetries}")
    require(highWater >= 0, s"journal highWater must be >= 0, got $highWater")
    new DeliveryRecorder(store, config, highWater, registry, clock)
