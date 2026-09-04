package io.parapet.journal

import io.parapet.effect.{Clock, Effect}
import org.slf4j.LoggerFactory

import java.nio.file.Path

/** Local filesystem [[JournalStore]]. */
class JournalStoreLocal[F[_]](
    config: JournalStoreLocal.Config,
    clock: Clock = Clock()
)(using effect: Effect[F])
    extends JournalStore[F]:

  private val logger = LoggerFactory.getLogger(classOf[JournalStoreLocal[?]])
  private val log    = new DeliveryLog(
    DeliveryLog.Config(config.dataDir, config.maxSegmentBytes, config.maxEntryBytes),
    clock = clock
  )

  override def append(entries: Vector[JournalEntry]): F[Unit] =
    effect.delay {
      if entries.nonEmpty then
        logger.debug(s"append journal entries [${entries.head.seq}, ${entries.last.seq}] (${entries.size} entries)")
        log.append(entries)
    }

  override def read(afterSeq: Long): F[Vector[JournalEntry]] =
    effect.delay(log.read(afterSeq))

  override def maxSeq: F[Option[Long]] =
    effect.delay(log.maxSeq)

  override def maxEnvelopeId: F[Option[Long]] =
    effect.delay(log.maxEnvelopeId)

  override def truncate(upToSeq: Long): F[Unit] =
    effect.delay(log.truncate(upToSeq))

object JournalStoreLocal:

  val DefaultMaxSegmentBytes: Long = 32L * 1024L * 1024L
  val DefaultMaxEntryBytes: Int    = 16 * 1024 * 1024

  private[parapet] val MinimumSegmentBytes: Long = DeliveryLog.MinimumSegmentBytes

  final case class Config(
      dataDir: Path,
      maxSegmentBytes: Long = DefaultMaxSegmentBytes,
      maxEntryBytes: Int = DefaultMaxEntryBytes
  )
