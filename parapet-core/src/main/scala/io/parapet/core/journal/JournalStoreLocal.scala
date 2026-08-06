package io.parapet.core.journal

import io.parapet.effect.Effect
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Path, StandardCopyOption}
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** Local filesystem [[JournalStore]]: one file per batch, named by its `[minSeq]-[maxSeq]` range. */
class JournalStoreLocal[F[_]](config: JournalStoreLocal.Config)(using effect: Effect[F]) extends JournalStore[F]:

  import JournalStoreLocal.*

  private val logger = LoggerFactory.getLogger(classOf[JournalStoreLocal[?]])

  override def append(batch: Seq[JournalEntry]): F[Unit] =
    effect.delay {
      if batch.nonEmpty then
        val minSeq = batch.iterator.map(_.seq).min
        val maxSeq = batch.iterator.map(_.seq).max
        logger.debug(s"append journal batch [$minSeq, $maxSeq] of ${batch.size} entries")
        Files.createDirectories(config.dataDir)
        val target = config.dataDir.resolve(fileName(minSeq, maxSeq))
        val temp   = Files.createTempFile(config.dataDir, "pjrn-", ".tmp")
        try
          Files.write(temp, JournalEntryBinaryFormat.encodeBatch(batch))
          Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
          ()
        finally
          Try(Files.deleteIfExists(temp))
          ()
    }

  override def read(afterSeq: Long): F[Vector[JournalEntry]] =
    effect.delay {
      segments
        .filter(_.maxSeq > afterSeq)
        .flatMap(segment => JournalEntryBinaryFormat.decodeBatch(Files.readAllBytes(segment.path)))
        .filter(_.seq > afterSeq)
        .sortBy(_.seq)
    }

  override def maxSeq: F[Option[Long]] =
    effect.delay(segments.map(_.maxSeq).maxOption)

  override def maxEnvelopeId: F[Option[Long]] =
    effect.delay {
      // id isn't encoded in the filename (and isn't monotonic with seq), so this decodes every segment.
      segments
        .flatMap(segment => JournalEntryBinaryFormat.decodeBatch(Files.readAllBytes(segment.path)))
        .flatMap(entry => Vector(entry.id, entry.cause))
        .maxOption
    }

  override def truncate(upToSeq: Long): F[Unit] =
    effect.delay {
      segments.filter(_.maxSeq <= upToSeq).foreach { segment =>
        Files.deleteIfExists(segment.path)
        ()
      }
    }

  /** Segment files sorted by `minSeq`; empty when the directory doesn't exist. Fails if the path exists but is not a
    * directory.
    */
  private def segments: Vector[Segment] =
    if !Files.exists(config.dataDir) then Vector.empty
    else if !Files.isDirectory(config.dataDir) then
      throw new java.nio.file.NotDirectoryException(config.dataDir.toString)
    else
      val listing = Files.list(config.dataDir)
      try
        listing
          .iterator()
          .asScala
          .filter(_.getFileName.toString.endsWith(Suffix))
          .map { path =>
            val (minSeq, maxSeq) = parseRange(path.getFileName.toString)
            Segment(path, minSeq, maxSeq)
          }
          .toVector
          .sortBy(_.minSeq)
      finally listing.close()

object JournalStoreLocal:

  case class Config(dataDir: Path)

  final private case class Segment(path: Path, minSeq: Long, maxSeq: Long)

  private val Suffix   = ".jrnl"
  private val SeqWidth = 20 // decimal digits of Long.MaxValue

  private def fileName(minSeq: Long, maxSeq: Long): String =
    s"${pad(minSeq)}-${pad(maxSeq)}$Suffix"

  private def pad(seq: Long): String =
    String.format(s"%0${SeqWidth}d", seq)

  private def parseRange(name: String): (Long, Long) =
    val range = name.stripSuffix(Suffix).split('-') match
      case Array(min, max) => min.toLongOption.zip(max.toLongOption)
      case _               => None
    range.getOrElse(throw new IllegalStateException(s"malformed journal segment file: $name"))
