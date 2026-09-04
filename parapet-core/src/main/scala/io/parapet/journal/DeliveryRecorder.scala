package io.parapet.journal

import io.parapet.ProcessRef
import io.parapet.effect.Monad.*
import io.parapet.effect.Effect

import scala.util.{Failure, Success}

/** Records deliveries to a [[JournalStore]], assigning each a global delivery `seq`.
  *
  * Admission is totally ordered: `seq` is assigned in the same step that admits the delivery, so admission order equals
  * `seq` order. Calls to [[advanceSequence]] may leave gaps between recorded positions.
  *
  * In [[JournalWriteMode.Buffered]] mode, an [[admit]] that fills a batch waits for that batch to be stored. In
  * [[JournalWriteMode.WriteAhead]] mode, every [[admit]] waits for its delivery and all earlier admissions to be
  * stored. A write failure is terminal and causes every in-flight and subsequent operation to fail.
  */
final class DeliveryRecorder[F[_]] private (
    store: JournalStore[F],
    registry: EventCodecRegistry,
    config: JournalConfig,
    startSeq: Long
)(using effect: Effect[F]):

  import DeliveryRecorder.*

  private val recorder = Recorder[F, EncodedDraft](
    new DeliveryStore[F](store),
    Recorder.Config(startId = startSeq, batchSize = config.batchSize)
  )

  /** Establishes the delivery's global position and admits it under the configured durability guarantee. */
  def admit(draft: JournalDraft): F[Long] =
    encode(draft).flatMap { encoded =>
      config.writeMode match
        case JournalWriteMode.Buffered   => recorder.admit(encoded)
        case JournalWriteMode.WriteAhead => recorder.admitAndFlush(encoded)
    }

  /** Establishes the delivery's global position and waits until it and all earlier admissions have been stored. */
  def admitAndFlush(draft: JournalDraft): F[Long] =
    encode(draft).flatMap(encoded => recorder.admitAndFlush(encoded))

  private def encode(draft: JournalDraft): F[EncodedDraft] =
    effect.suspend {
      registry.codecFor(draft.event) match
        case None =>
          effect.raiseError(new IllegalStateException(s"no journal codec for event ${draft.event.getClass.getName}"))
        case Some(codec) =>
          codec.encode(draft.event) match
            case Success(bytes) =>
              effect.pure(
                EncodedDraft(
                  draft.id,
                  draft.sender,
                  draft.receiver,
                  draft.cause,
                  bytes.clone(),
                  codec.tag,
                  codec.version
                )
              )
            case Failure(error) => effect.raiseError(error)
    }

  /** Assigns and returns the next delivery sequence without admitting a delivery. */
  def advanceSequence(): F[Long] = recorder.advanceId()

  /** Publishes all deliveries admitted before this call returns. */
  def flush(): F[Unit] = recorder.flush()

  /** Publishes preceding admissions, then discards durable segments covered by `upToSeq`. */
  def truncate(upToSeq: Long): F[Unit] =
    flush() >> store.truncate(upToSeq)

  /** Rejects new admissions, publishes all admitted deliveries, and closes the recorder. */
  def close(): F[Unit] = recorder.close()

  /** Ensures that subsequently assigned delivery sequences are greater than `after`. */
  def continueAfter(after: Long): Unit = recorder.continueAfter(after)

  /** All durably recorded entries with `seq > afterSeq`, in ascending `seq` order. */
  def read(afterSeq: Long): F[Vector[JournalEntry]] = store.read(afterSeq)

  /** The durable delivery-sequence high-water, including discarded entries, if any. */
  def maxSeq: F[Option[Long]] = store.maxSeq

  /** The durable envelope-id high-water, including discarded entries, if any. */
  def maxEnvelopeId: F[Option[Long]] = store.maxEnvelopeId

object DeliveryRecorder:

  final private case class EncodedDraft(
      id: Long,
      sender: ProcessRef.Unknown,
      receiver: ProcessRef.Unknown,
      cause: Long,
      event: Array[Byte],
      tag: String,
      schemaVersion: Int
  )

  final private class DeliveryStore[F[_]](store: JournalStore[F])(using effect: Effect[F])
      extends Recorder.Store[F, EncodedDraft]:

    def append(entries: Vector[Recorder.Entry[EncodedDraft]]): F[Unit] =
      effect
        .delay {
          entries.map { entry =>
            val draft = entry.data
            JournalEntry(
              entry.id,
              draft.id,
              draft.sender,
              draft.receiver,
              draft.cause,
              draft.event,
              draft.tag,
              draft.schemaVersion
            )
          }
        }
        .flatMap(store.append)

  /** Creates a recorder for a journal with no previously assigned delivery positions. */
  def fresh[F[_]](
      store: JournalStore[F],
      registry: EventCodecRegistry = EventCodecRegistry.empty,
      config: JournalConfig = JournalConfig.default
  )(using Effect[F]): DeliveryRecorder[F] =
    create(store, config, 0L, registry)

  /** Creates a recorder that continues after `highWater`.
    *
    * @param highWater
    *   the highest delivery position already represented by recovered durable state.
    */
  def resume[F[_]](
      store: JournalStore[F],
      highWater: Long,
      registry: EventCodecRegistry = EventCodecRegistry.empty,
      config: JournalConfig = JournalConfig.default
  )(using Effect[F]): DeliveryRecorder[F] =
    create(store, config, highWater, registry)

  private def create[F[_]](
      store: JournalStore[F],
      config: JournalConfig,
      highWater: Long,
      registry: EventCodecRegistry
  )(using Effect[F]): DeliveryRecorder[F] =
    new DeliveryRecorder[F](store, registry, config, highWater)
