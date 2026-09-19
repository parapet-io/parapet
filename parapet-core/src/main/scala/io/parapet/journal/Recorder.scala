package io.parapet.journal

import io.parapet.effect.Monad.*
import io.parapet.effect.{Deferred, Effect}

import java.util.concurrent.LinkedBlockingQueue

import Recorder.*

/** Assigns monotonically increasing identifiers and publishes admitted values through a dedicated writer.
  *
  * [[runWriter]] must be supervised alongside every operation on this recorder. A store failure fails the writer, every
  * operation waiting for durability, and all subsequent recorder operations.
  */
final class Recorder[F[_], A] private (
    store: Store[F, A],
    config: Config
)(using effect: Effect[F]):

  private type Completion = Deferred[F, Either[Throwable, Unit]]

  final private class Batch(
      var entries: Array[Entry[A]],
      var size: Int,
      var requested: Boolean,
      val completion: Completion
  )

  sealed private trait Work
  final private case class Write(batch: Batch) extends Work
  private case object Stop                     extends Work

  sealed private trait Phase
  private object Phase:
    case object Open                           extends Phase
    final case class Closing(done: Completion) extends Phase
    case object Closed                         extends Phase
    final case class Failed(error: Throwable)  extends Phase

  private val closedError = new IllegalStateException("recorder is closed")
  private val idExhausted = new IllegalStateException("recorder identifier exhausted at Long.MaxValue")

  private val lock  = new Object
  private val queue = new LinkedBlockingQueue[Work]()

  private var phase: Phase    = Phase.Open
  private var id: Long        = config.startId
  private var active          = newBatch(new Array[Entry[A]](config.batchSize))
  private var tail: Batch     = null
  private var inFlight: Batch = null
  private var writerClaimed   = false

  /** Assigns the next identifier and admits `data` without waiting for it to become durable.
    *
    * @return
    *   the identifier assigned to `data`.
    */
  def admit(data: A): F[Long] =
    admit(data, durable = false)

  /** Assigns the next identifier and waits until the batch containing `data` is durable.
    *
    * @return
    *   the identifier assigned to `data`.
    */
  def admitDurable(data: A): F[Long] =
    admit(data, durable = true)

  /** Alias for [[admitDurable]]. */
  def admitAndFlush(data: A): F[Long] =
    admitDurable(data)

  /** Assigns and returns the next identifier without admitting a value.
    *
    * The assigned identifier remains consumed, so a subsequent admission receives a greater identifier.
    */
  def advanceId(): F[Long] =
    effect.suspend {
      lock.synchronized {
        phase match
          case Phase.Failed(error)             => effect.raiseError(error)
          case Phase.Closing(_) | Phase.Closed => effect.raiseError(closedError)
          case Phase.Open                      =>
            if id == Long.MaxValue then effect.raiseError(idExhausted)
            else
              id += 1
              effect.pure(id)
      }
    }

  /** Waits until every value admitted before this call is durable.
    *
    * Fails with the recorder's terminal error when publication has failed, and rejects calls after a successful close.
    */
  def flush(): F[Unit] =
    effect.suspend {
      lock.synchronized {
        phase match
          case Phase.Failed(error) => effect.raiseError(error)
          case Phase.Closing(done) => await(done)
          case Phase.Closed        => effect.raiseError(closedError)
          case Phase.Open          =>
            if active.size > 0 then requestWriteLocked(active)
            if tail == null then effect.pure(()) else await(tail.completion)
      }
    }

  /** Rejects new admissions, drains every admitted value, and stops [[runWriter]].
    *
    * Concurrent calls share the same completion, and calls after a successful close are no-ops.
    */
  def close(): F[Unit] =
    effect.suspend {
      lock.synchronized {
        phase match
          case Phase.Failed(error) => effect.raiseError(error)
          case Phase.Closing(done) => await(done)
          case Phase.Closed        => effect.pure(())
          case Phase.Open          =>
            val done = Deferred.unsafe[F, Either[Throwable, Unit]]()
            phase = Phase.Closing(done)
            if active.size > 0 then requestWriteLocked(active)
            queue.add(Stop)
            await(done)
      }
    }

  /** Publishes requested batches until [[close]] is called.
    *
    * Exactly one invocation is allowed. The caller owns and supervises this long-running effect.
    */
  def runWriter: F[Unit] =
    effect.suspend {
      claimWriter()
      effect.guarantee(
        writerLoop().handleErrorWith { error =>
          effect.delay(fail(error)).flatMap(effectiveError => effect.raiseError(effectiveError))
        }
      )(effect.delay(settleWriterExit()))
    }

  /** Ensures that subsequently assigned identifiers are greater than `after`.
    *
    * Has no effect when the current identifier is already greater than or equal to `after`.
    */
  def continueAfter(after: Long): Unit =
    lock.synchronized { if after > id then id = after }

  /** Implements buffered and durable admission.
    *
    * Identifier assignment and insertion are one atomic operation. When `durable` is true, the returned effect also
    * waits for publication of the containing batch.
    */
  private def admit(data: A, durable: Boolean): F[Long] =
    effect.suspend {
      lock.synchronized {
        phase match
          case Phase.Failed(error)             => effect.raiseError(error)
          case Phase.Closing(_) | Phase.Closed => effect.raiseError(closedError)
          case Phase.Open                      =>
            if id == Long.MaxValue then effect.raiseError(idExhausted)
            else
              id += 1
              val assignedId = id
              val batch      = active
              batch.entries(batch.size) = Entry(assignedId, data)
              batch.size += 1

              val full = batch.size == config.batchSize
              if full then sealActiveLocked(batch)
              if full || durable then requestWriteLocked(batch)

              if durable then await(batch.completion).as(assignedId)
              else effect.pure(assignedId)
      }
    }

  /** Requests publication of `batch` exactly once and makes it the latest durability barrier.
    *
    * The caller must hold the recorder lock.
    */
  private def requestWriteLocked(batch: Batch): Unit =
    if !batch.requested then
      batch.requested = true
      tail = batch
      queue.add(Write(batch))
      ()

  /** Returns the admitted entries that the writer must publish for `batch`.
    *
    * If `batch` still accepts admissions, this method closes it first. Entries are returned in identifier order.
    */
  private def prepareWrite(batch: Batch): Vector[Entry[A]] =
    lock.synchronized {
      if batch eq active then sealActiveLocked(batch)
      Vector.tabulate(batch.size)(batch.entries(_))
    }

  /** Stops admission into `batch` and installs an empty active batch.
    *
    * This method neither queues nor publishes `batch`. The caller must hold the recorder lock, and `batch` must be the
    * current non-empty active batch.
    *
    * @throws IllegalStateException
    *   if `batch` is not the active batch or contains no entries.
    */
  private def sealActiveLocked(batch: Batch): Unit =
    if !(batch eq active) then throw new IllegalStateException("only the active recorder batch can be sealed")
    if batch.size <= 0 then throw new IllegalStateException("an empty recorder batch cannot be sealed")

    if batch.size == config.batchSize then active = newBatch(new Array[Entry[A]](config.batchSize))
    else
      // Keep only the occupied prefix with the closed batch and reuse its original capacity for new admissions.
      val reusable = batch.entries
      val compact  = new Array[Entry[A]](batch.size)
      System.arraycopy(reusable, 0, compact, 0, batch.size)
      batch.entries = compact
      active = newBatch(reusable)

  /** Processes publication requests in FIFO order until a close request is reached.
    *
    * A batch completion is signalled only after [[Store.append]] succeeds. Any failure escapes to [[runWriter]], which
    * transitions the recorder to its terminal failed state.
    */
  private def writerLoop(): F[Unit] =
    effect.blocking(takeWork()).flatMap {
      case Write(batch) =>
        effect.delay(prepareWrite(batch)).flatMap(store.append) >>
          effect.delay(completeWrite(batch)) >>
          writerLoop()
      case Stop => effect.delay(completeClose())
    }

  /** Waits for the next writer command.
    *
    * A returned write request remains covered by terminal failure handling if the writer subsequently exits.
    */
  private def takeWork(): Work =
    val work = queue.take()
    work match
      case Write(batch) => lock.synchronized { inFlight = batch }
      case Stop         => ()
    work

  /** Marks `batch` durable and releases every operation waiting for its publication. */
  private def completeWrite(batch: Batch): Unit =
    lock.synchronized {
      inFlight = null
      if tail eq batch then tail = null
      batch.completion.unsafeComplete(Right(()))
    }

  /** Completes an orderly close after all preceding publication requests have succeeded.
    *
    * @throws IllegalStateException
    *   if the writer receives a close command while the recorder is still open.
    */
  private def completeClose(): Unit =
    lock.synchronized {
      phase match
        case Phase.Closing(done) =>
          phase = Phase.Closed
          done.unsafeComplete(Right(()))
        case Phase.Failed(_) | Phase.Closed => ()
        case Phase.Open                     =>
          throw new IllegalStateException("writer received stop while recorder was open")
    }

  /** Reserves the writer role for the current [[runWriter]] invocation.
    *
    * @throws IllegalStateException
    *   if a writer has already been claimed or the recorder is closed.
    */
  private def claimWriter(): Unit =
    lock.synchronized {
      if writerClaimed then throw new IllegalStateException("recorder writer is already running or has terminated")
      phase match
        case Phase.Failed(error) => throw error
        case Phase.Closed        => throw closedError
        case _                   => writerClaimed = true
    }

  /** Converts writer termination before an orderly close into a terminal recorder failure. */
  private def settleWriterExit(): Unit =
    lock.synchronized {
      phase match
        case Phase.Closed | Phase.Failed(_) => ()
        case _                              =>
          failLocked(new IllegalStateException("recorder writer stopped before close"))
          ()
    }

  /** Fails the recorder and returns the first error that made it terminal. */
  private def fail(error: Throwable): Throwable =
    lock.synchronized(failLocked(error))

  /** Performs the terminal failure transition and releases every durability waiter.
    *
    * The first failure is retained as the recorder's terminal error. The caller must hold the recorder lock.
    */
  private def failLocked(error: Throwable): Throwable =
    phase match
      case Phase.Failed(existing) => existing
      case previous               =>
        phase = Phase.Failed(error)
        if inFlight != null then inFlight.completion.unsafeComplete(Left(error))
        if active.requested then active.completion.unsafeComplete(Left(error))

        var work = queue.poll()
        while work != null do
          work match
            case Write(batch) => batch.completion.unsafeComplete(Left(error))
            case Stop         => ()
          work = queue.poll()

        previous match
          case Phase.Closing(done) => done.unsafeComplete(Left(error))
          case _                   => ()

        clearActiveLocked()
        tail = null
        inFlight = null
        error

  /** Discards values that remain in the active batch after a terminal failure.
    *
    * The caller must hold the recorder lock.
    */
  private def clearActiveLocked(): Unit =
    var index = 0
    while index < active.size do
      active.entries(index) = null
      index += 1
    active.size = 0

  /** Waits for a durability completion and raises its recorded failure, if any. */
  private def await(completion: Completion): F[Unit] =
    completion.get.flatMap {
      case Right(_)    => effect.pure(())
      case Left(error) => effect.raiseError(error)
    }

  /** Creates an empty batch whose capacity is provided by `entries`. */
  private def newBatch(entries: Array[Entry[A]]): Batch =
    new Batch(
      entries = entries,
      size = 0,
      requested = false,
      completion = Deferred.unsafe[F, Either[Throwable, Unit]]()
    )

object Recorder:

  /** A value and its recorder-local admission identifier. */
  final case class Entry[A](id: Long, data: A)

  /** Appends recorder entries in the supplied order. Successful completion means every entry is durable. */
  trait Store[F[_], A]:
    /** Publishes `entries` in order and completes only after they satisfy the store's durability contract. */
    def append(entries: Vector[Entry[A]]): F[Unit]

  /** Recorder identifier and buffering configuration. */
  final case class Config(startId: Long, batchSize: Int)

  /** Creates a recorder whose first assigned identifier is `config.startId + 1`. */
  def apply[F[_], A](store: Store[F, A], config: Config)(using Effect[F]): Recorder[F, A] =
    require(config.startId >= 0, s"recorder startId must be >= 0, got ${config.startId}")
    require(config.batchSize > 0, s"recorder batchSize must be > 0, got ${config.batchSize}")
    new Recorder[F, A](store, config)
