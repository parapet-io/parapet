package io.parapet.journal

import com.typesafe.scalalogging.Logger
import io.parapet.effect.Monad.*
import io.parapet.effect.{Deferred, Effect}
import org.slf4j.LoggerFactory

import java.util.ArrayDeque
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import Recorder.*

/** Assigns monotonically increasing identifiers to admitted values and publishes them to a [[Store]] in order.
  *
  * Admission order equals identifier order within a recorder. Values are published on reaching `batchSize`, on
  * [[flush]], and on [[close]]. An admission that does not complete a batch may return before its value is published. A
  * store failure or interrupted publication is terminal and causes subsequent operations to fail.
  */
final class Recorder[F[_], A] private (
    store: Store[F, A],
    config: Config
)(using effect: Effect[F]):

  private val logger       = Logger(LoggerFactory.getLogger(getClass))
  private val pollInterval = 1.milli

  private type SealedSlot = Deferred[F, Either[Throwable, Unit]]

  final private case class SealedBatch(entries: Array[Entry[A]], size: Int, completion: SealedSlot)

  sealed private trait Phase
  private object Phase:
    case object Open                          extends Phase
    case object Closing                       extends Phase
    case object Closed                        extends Phase
    final case class Failed(error: Throwable) extends Phase

  sealed private trait AdmitOutcome
  private object AdmitOutcome:
    final case class Rejected(error: Throwable)                             extends AdmitOutcome
    final case class Buffered(id: Long)                                     extends AdmitOutcome
    final case class Sealed(id: Long, batch: SealedBatch, claimed: Boolean) extends AdmitOutcome

  sealed private trait FlushOutcome
  private object FlushOutcome:
    final case class Rejected(error: Throwable)                 extends FlushOutcome
    case object Empty                                           extends FlushOutcome
    final case class Tail(batch: SealedBatch, claimed: Boolean) extends FlushOutcome

  sealed private trait CloseOutcome
  private object CloseOutcome:
    case object AlreadyClosed                  extends CloseOutcome
    final case class Failed(error: Throwable)  extends CloseOutcome
    final case class Proceed(claimed: Boolean) extends CloseOutcome

  final private class DrainOwnerAborted
      extends RuntimeException("recorder drain owner aborted before settling publication")

  private val closedError = new IllegalStateException("recorder is closed")
  private val idExhausted = new IllegalStateException("recorder identifier exhausted at Long.MaxValue")

  // Implementation:
  //
  // Identifier allocation and buffer insertion happen under `lock` in one step (so admission order == id order); the
  // lock never covers store work. Publication is inline: a sealing operation conditionally claims drain ownership - an
  // identity token stored in `owner` - and if it wins drives the store writes itself, one batch at a time in FIFO order;
  // a non-owner instead awaits its batch's completion.
  //
  // Ownership is claimed *inside* an `effect.guarantee` scope whose finalizer (`settleOwnerExit`) releases it, so a
  // cancelled or otherwise abnormally-exited owner cannot leave the FIFO owned-but-idle. If that finalizer still holds
  // the token, the drain did not settle, so `fail` runs - transition to Failed and complete every pending batch - which
  // is why interrupted publication is terminal. The identity token (rather than a boolean) prevents a stale finalizer
  // from releasing a newer owner.
  //
  // The owner drains until the FIFO is empty, not just until its own batch is published, so under continuous traffic one
  // caller can remain the writer and an owning `flush` can wait for batches admitted after it.
  //
  // Everything below is guarded by `lock`. `owner` is the current drain-owner token, or null when no one is draining.
  private val lock            = new Object
  private var phase: Phase    = Phase.Open
  private var id: Long        = config.startId
  private var active          = new Array[Entry[A]](config.batchSize)
  private var activeSize: Int = 0
  private val ready           = new ArrayDeque[SealedBatch]()
  private var owner: AnyRef   = null

  /** Assigns the next identifier and admits `data`, returning the assigned identifier.
    *
    * An admission that fills a batch waits for that batch to be published. Other admissions are buffered until a later
    * admission fills the batch, or [[flush]] or [[close]] publishes it.
    */
  def admit(data: A): F[Long] =
    admit(data, sealAfterAdmission = false)

  /** Assigns the next identifier, admits `data`, and publishes through the batch containing it before returning. */
  def admitAndFlush(data: A): F[Long] =
    admit(data, sealAfterAdmission = true)

  private def admit(data: A, sealAfterAdmission: Boolean): F[Long] =
    effect.suspend {
      val token = new Object
      effect.guarantee(
        effect.delay(admit(data, token, sealAfterAdmission)).flatMap {
          case AdmitOutcome.Rejected(error)                    => effect.raiseError(error)
          case AdmitOutcome.Buffered(assignedId)               => effect.pure(assignedId)
          case AdmitOutcome.Sealed(assignedId, batch, claimed) =>
            (drainIfOwner(claimed) >> awaitBatch(batch)).as(assignedId)
        }
      )(settleOwnerExit(token))
    }

  /** Assigns and returns the next identifier without admitting a value. */
  def advanceId(): F[Long] =
    effect.suspend {
      lock.synchronized {
        phase match
          case Phase.Failed(error)          => effect.raiseError(error)
          case Phase.Closing | Phase.Closed => effect.raiseError(closedError)
          case Phase.Open                   =>
            if id == Long.MaxValue then effect.raiseError(idExhausted)
            else
              id += 1
              effect.pure(id)
      }
    }

  /** Publishes all values admitted before this call returns. */
  def flush(): F[Unit] =
    effect.suspend {
      val token = new Object
      effect.guarantee(
        effect.delay(flush(token)).flatMap {
          case FlushOutcome.Rejected(error)      => effect.raiseError(error)
          case FlushOutcome.Empty                => effect.pure(())
          case FlushOutcome.Tail(batch, claimed) => drainIfOwner(claimed) >> awaitBatch(batch)
        }
      )(settleOwnerExit(token))
    }

  /** Rejects new admissions, publishes all admitted values, and closes the recorder. */
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

  /** Ensures that subsequently assigned identifiers are greater than `after`. */
  def continueAfter(after: Long): Unit =
    lock.synchronized { if after > id then id = after }

  /** Drives the FIFO if this operation claimed ownership; a no-op otherwise. */
  private def drainIfOwner(claimed: Boolean): F[Unit] =
    if claimed then drainLoop() else effect.pure(())

  private def admit(data: A, token: AnyRef, sealAfterAdmission: Boolean): AdmitOutcome =
    lock.synchronized {
      phase match
        case Phase.Failed(error)          => AdmitOutcome.Rejected(error)
        case Phase.Closing | Phase.Closed => AdmitOutcome.Rejected(closedError)
        case Phase.Open                   =>
          if id == Long.MaxValue then AdmitOutcome.Rejected(idExhausted)
          else
            id += 1
            val assignedId = id
            active(activeSize) = Entry(assignedId, data)
            activeSize += 1
            if sealAfterAdmission || activeSize >= config.batchSize then
              AdmitOutcome.Sealed(assignedId, sealLocked(), claimLocked(token))
            else AdmitOutcome.Buffered(assignedId)
    }

  private def flush(token: AnyRef): FlushOutcome =
    lock.synchronized {
      phase match
        case Phase.Failed(error) => FlushOutcome.Rejected(error)
        case Phase.Closed        => FlushOutcome.Rejected(closedError)
        case _                   =>
          if activeSize > 0 then sealLocked()
          if ready.isEmpty then FlushOutcome.Empty
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
  private def sealLocked(): SealedBatch =
    val batch = SealedBatch(active, activeSize, Deferred.unsafe[F, Either[Throwable, Unit]]())
    active = new Array[Entry[A]](config.batchSize)
    activeSize = 0
    ready.addLast(batch)
    batch

  // Releases buffered entries so a terminal recorder does not retain them. Caller must hold `lock`.
  private def clearActiveLocked(): Unit =
    var i = 0
    while i < activeSize do
      active(i) = null
      i += 1
    activeSize = 0

  // Claims drain ownership for `token` if unowned. Caller must hold `lock`.
  private def claimLocked(token: AnyRef): Boolean =
    if owner == null then
      owner = token
      true
    else false

  // ---- publication (no lock held during store IO) ----

  // Runs after the guaranteed body. If the token is still held, publication was interrupted, so fail closed.
  private def settleOwnerExit(token: AnyRef): F[Unit] =
    effect.suspend {
      if lock.synchronized(token eq owner) then fail(new DrainOwnerAborted)
      else effect.pure(())
    }

  // Drives the FIFO to empty or to the first failure.
  private def drainLoop(): F[Unit] =
    drainBatches().handleErrorWith(error => fail(error) >> effect.raiseError(error))

  private def drainBatches(): F[Unit] =
    effect.delay(nextBatch()).flatMap {
      case None        => effect.pure(())
      case Some(batch) => store0(batch) >> completeHead(batch) >> drainBatches()
    }

  private def nextBatch(): Option[SealedBatch] =
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

  private def completeHead(batch: SealedBatch): F[Unit] =
    effect.delay {
      lock.synchronized {
        batch.completion.unsafeComplete(Right(()))
        ready.removeFirst()
        ()
      }
    }

  private def store0(batch: SealedBatch): F[Unit] =
    // The buffer is never mutated after sealing, so it can be copied without holding the admission lock.
    store.append(Vector.tabulate(batch.size)(batch.entries(_)))

  // Transitions to Failed (first error wins) and completes every pending batch with that error.
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
            clearActiveLocked()
            error
        (eff, ready.iterator().asScala.toVector)
      }
      logger.error("recorder failed; failing every pending batch", effectiveError)
      pending.foldLeft(effect.pure(()))((acc, b) => acc >> b.completion.complete(Left(effectiveError)).void) >>
        effect.delay(lock.synchronized {
          ready.clear()
          owner = null
        })
    }

  private def awaitBatch(batch: SealedBatch): F[Unit] =
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

object Recorder:

  /** A value and its recorder-local admission identifier. */
  final case class Entry[A](id: Long, data: A)

  /** Appends recorder entries in the supplied order. Successful completion means every entry is durable. */
  trait Store[F[_], A]:
    def append(entries: Vector[Entry[A]]): F[Unit]

  /** Recorder identifier and buffering configuration. */
  final case class Config(startId: Long, batchSize: Int)

  /** Creates a recorder whose first assigned identifier is `config.startId + 1`. */
  def apply[F[_], A](store: Store[F, A], config: Config)(using Effect[F]): Recorder[F, A] =
    require(config.startId >= 0, s"recorder startId must be >= 0, got ${config.startId}")
    require(config.batchSize > 0, s"recorder batchSize must be > 0, got ${config.batchSize}")
    new Recorder[F, A](store, config)
