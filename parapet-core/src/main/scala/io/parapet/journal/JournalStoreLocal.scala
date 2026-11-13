package io.parapet.journal

import io.parapet.effect.{Clock, Effect}
import io.parapet.journal.HighWaterStore.HighWater
import io.parapet.journal.log.{DeliveryLog, Log}
import org.slf4j.LoggerFactory

import java.nio.file.Path

/** Local filesystem [[JournalStore]]: a [[DeliveryLog]] in `config.dataDir`. Only one store may use a directory at a
  * time.
  *
  * [[truncate]] deletes sealed segments only, so the entries of the segment being written stay until it is sealed.
  * After a failed append the store must not be used again; nothing here guards against it.
  */
class JournalStoreLocal[F[_]](
    config: JournalStoreLocal.Config,
    clock: Clock = Clock()
)(using effect: Effect[F])
    extends JournalStore[F]:

  private val logger = LoggerFactory.getLogger(classOf[JournalStoreLocal[?]])
  private val log    = new DeliveryLog(
    Log.Config(config.dataDir, config.maxSegmentBytes, config.durability, maxEntrySize = config.maxEntryBytes),
    new HighWaterStore(config.dataDir),
    clock
  )

  // The log is not thread-safe: it and the fields below are used one caller at a time.
  private var opened = false
  // Read from the log when the store is opened, then raised by every append.
  private var highWater = Option.empty[HighWater]

  /** Opens the log, which seals what the previous run was writing, and reads the high water from it. */
  override def open(): F[Unit] =
    effect.delay {
      synchronized {
        require(!opened, "the journal store is already open")
        log.init()
        highWater = log.highWater()
        opened = true
      }
    }

  override def append(entries: Vector[JournalEntry]): F[Unit] =
    use {
      if entries.nonEmpty then
        logger.debug(s"append journal entries [${entries.head.seq}, ${entries.last.seq}] (${entries.size} entries)")
        checkSequences(entries)
        entries.foreach(entry => log.append(entry).get)
        log.sync().get
        val appended = entries.map(entry => HighWater(entry.seq, math.max(entry.id, entry.cause))).reduce(_.merge(_))
        highWater = Some(highWater.fold(appended)(_.merge(appended)))
    }

  override def read(afterSeq: Long): F[Vector[JournalEntry]] =
    use(log.read(afterSeq))

  override def maxSeq: F[Option[Long]] =
    use(highWater.map(_.deliverySeq))

  override def maxEnvelopeId: F[Option[Long]] =
    use(highWater.map(_.envelopeId))

  override def truncate(upToSeq: Long): F[Unit] =
    use(log.truncate(upToSeq))

  override def close(): F[Unit] =
    effect.delay {
      synchronized {
        log.close()
        opened = false
      }
    }

  /** Runs `body` on the open log, one caller at a time. */
  private def use[A](body: => A): F[A] =
    effect.delay {
      synchronized {
        require(opened, "the journal store is not open")
        body
      }
    }

  /** The sequences must increase and follow the high water, which deleted entries still count towards. */
  private def checkSequences(entries: Vector[JournalEntry]): Unit =
    require(entries.head.seq >= 0, s"negative delivery sequence: ${entries.head.seq}")
    entries.sliding(2).foreach {
      case Vector(previous, next) =>
        require(next.seq > previous.seq, s"delivery sequences must increase: ${previous.seq} then ${next.seq}")
      case _ => ()
    }
    highWater.foreach { high =>
      if entries.head.seq <= high.deliverySeq then
        throw new IllegalStateException(
          s"delivery sequence ${entries.head.seq} does not follow the high water ${high.deliverySeq}"
        )
    }

object JournalStoreLocal:

  val DefaultMaxSegmentBytes: Long = 32L * 1024L * 1024L
  val DefaultMaxEntryBytes: Int    = 16 * 1024 * 1024

  /** Local delivery-log configuration.
    *
    * @param dataDir
    *   directory containing delivery-log files.
    * @param maxSegmentBytes
    *   size at which the active segment is sealed: an entry that would take it past this starts the next segment.
    * @param maxEntryBytes
    *   maximum encoded size accepted for one delivery.
    * @param durability
    *   storage guarantee acknowledged by a successful append.
    */
  final case class Config(
      dataDir: Path,
      maxSegmentBytes: Long = DefaultMaxSegmentBytes,
      maxEntryBytes: Int = DefaultMaxEntryBytes,
      durability: JournalDurability = JournalDurability.HostCrash
  )
