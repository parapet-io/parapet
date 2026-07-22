package io.parapet.core.snapshot

import io.parapet.ProcessRef.Unknown
import io.parapet.effect.Effect

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, StandardCopyOption}
import java.security.MessageDigest
import java.util.HexFormat
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** Local filesystem [[SnapshotStorage]]: a directory per process, one file per snapshot named by zero-padded id so
  * lexicographic order equals id order. Snapshot bytes are produced/consumed by [[SnapshotBinaryFormat]].
  *
  * The directory name is `sha256(ref)` in hex - a fixed-length. Writes are atomic (temp file + rename): a crash
  * mid-write never leaves a partial snapshot behind.
  */
class SnapshotStorageLocal[F[_]](config: SnapshotStorageLocal.Config)(using effect: Effect[F])
    extends SnapshotStorage[F] {

  import SnapshotStorageLocal.*

  override def store(snapshot: Snapshot): F[Unit] =
    effect.delay {
      val dir = refDir(snapshot.metadata.processRef)
      Files.createDirectories(dir)
      val target = dir.resolve(fileName(snapshot.metadata.id))
      if (Files.exists(target)) {
        throw new IllegalStateException(
          s"snapshot file already exists: $target"
        )
      }
      val temp = Files.createTempFile(dir, "psnp-", ".tmp")
      try {
        Files.write(temp, SnapshotBinaryFormat.encode(snapshot))
        Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE)
        ()
      } finally {
        Try(Files.deleteIfExists(temp)) // ignore
        ()
      }
    }

  override def read(ref: Unknown, id: Long): F[Option[Snapshot]] =
    effect.delay {
      val file = refDir(ref).resolve(fileName(id))
      if (!Files.exists(file)) None
      else
        Some(SnapshotBinaryFormat.decode(Files.readAllBytes(file)))
    }

  override def latest(ref: Unknown): F[Option[Snapshot]] =
    effect.delay(find(ref, _ => true))

  override def latestBefore(ref: Unknown, atMillis: Long): F[Option[Snapshot]] =
    effect.delay(find(ref, _.metadata.createdAt <= atMillis))

  /** Newest snapshot satisfying `matches`, scanning newest-first and skipping corrupt entries. */
  private def find(ref: Unknown, matches: Snapshot => Boolean): Option[Snapshot] =
    snapshotFiles(refDir(ref)).reverseIterator
      .flatMap { file =>
        try {
          val snapshot = SnapshotBinaryFormat.decode(Files.readAllBytes(file))
          if (matches(snapshot)) Iterator.single(snapshot) else Iterator.empty
        } catch { case _: SnapshotBinaryFormat.CorruptSnapshotException => Iterator.empty }
      }
      .nextOption()

  private def refDir(ref: Unknown): Path =
    config.dataDir.resolve(sha256Hex(ref.value))

  /** Snapshot files of `dir` in ascending id order; empty when the directory doesn't exist. */
  private def snapshotFiles(dir: Path): Vector[Path] =
    if (!Files.isDirectory(dir)) Vector.empty
    else {
      val listing = Files.list(dir)
      try
        listing
          .iterator()
          .asScala
          .filter(_.getFileName.toString.endsWith(Suffix))
          .toVector
          .sortBy(_.getFileName.toString)
      finally listing.close()
    }
}

object SnapshotStorageLocal {

  case class Config(dataDir: Path)

  private val Suffix  = ".snap"
  private val IdWidth = 20 // decimal digits of Long.MaxValue

  private def fileName(id: Long): String =
    String.format(s"%0${IdWidth}d%s", id, Suffix)

  private def sha256Hex(value: String): String =
    require(value.nonEmpty)
    val digest =
      MessageDigest
        .getInstance("SHA-256")
        .digest(value.getBytes(UTF_8))
    HexFormat.of().formatHex(digest)
}
