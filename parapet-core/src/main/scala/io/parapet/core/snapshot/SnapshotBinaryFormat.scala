package io.parapet.core.snapshot

import io.parapet.{Event, ProcessRef}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util.zip.CRC32

/** The on-disk byte layout of a [[Snapshot]].
  *
  * A [[Snapshot]] encodes to a self-contained blob; any [[SnapshotStorage]] backend (filesystem, object store,
  * database) can persist that blob verbatim, so a snapshot written by one backend is readable by another.
  *
  * Layout (big-endian):
  * {{{
  * [magic "PSNP": Int][format version: Short]
  * [meta length: Int][meta][crc32(meta): Int]
  * [data length: Int][data][crc32(data): Int]
  * }}}
  *
  * `format version` is the layout version (this format); it is distinct from [[Snapshot.Metadata.schemaVersion]], which
  * versions the process's serialized state.
  */
object SnapshotBinaryFormat:

  /** A blob that cannot be decoded. */
  final class CorruptSnapshotException(message: String) extends RuntimeException(message)

  private val Magic: Int           = 0x50534e50 // "PSNP"
  private val FormatVersion: Short = 1

  private val MagicSize          = java.lang.Integer.BYTES
  private val VersionSize        = java.lang.Short.BYTES
  private val LengthSize         = java.lang.Integer.BYTES
  private val ChecksumSize       = java.lang.Integer.BYTES
  private val LongSize           = java.lang.Long.BYTES
  private val HeaderSize         = MagicSize + VersionSize
  private val MetadataLongFields = 5 // id, parentId, seq, createdAt, schemaVersion

  /** Encodes `snapshot` to a self-contained blob. */
  def encode(snapshot: Snapshot): Array[Byte] =
    val meta = encodeMetadata(snapshot.metadata)
    val data = snapshot.data
    val buf  = ByteBuffer.allocate(HeaderSize + sectionSize(meta.length) + sectionSize(data.length))
    buf.putInt(Magic)
    buf.putShort(FormatVersion)
    buf.putInt(meta.length)
    buf.put(meta)
    buf.putInt(crc32(meta))
    buf.putInt(data.length)
    buf.put(data)
    buf.putInt(crc32(data))
    buf.array()

  /** Decodes a blob to [[Snapshot]].
    *
    * @throws CorruptSnapshotException
    *   if the blob is not a snapshot.
    */
  def decode(bytes: Array[Byte]): Snapshot =
    val buf = ByteBuffer.wrap(bytes)
    check(buf.remaining() >= HeaderSize, "truncated header")
    check(buf.getInt() == Magic, "not a snapshot file (bad magic)")
    val version = buf.getShort()
    check(version == FormatVersion, s"unsupported format version: $version")
    val meta = readSection(buf, "metadata")
    val data = readSection(buf, "data")
    check(!buf.hasRemaining, "trailing bytes after data section")
    Snapshot(decodeMetadata(meta), data)

  /** Reads one `[length][bytes][crc32]` section, verifying the checksum. */
  private def readSection(buf: ByteBuffer, name: String): Array[Byte] =
    check(buf.remaining() >= LengthSize, s"truncated $name length")
    val length = buf.getInt()
    check(length >= 0 && buf.remaining() >= length + ChecksumSize, s"truncated $name section")
    val bytes = new Array[Byte](length)
    buf.get(bytes)
    check(crc32(bytes) == buf.getInt(), s"$name checksum mismatch")
    bytes

  private def encodeMetadata(metadata: Snapshot.Metadata): Array[Byte] =
    val refBytes = metadata.processRef.value.getBytes(UTF_8)
    val buf      = ByteBuffer.allocate(LengthSize + refBytes.length + MetadataLongFields * LongSize)
    buf.putInt(refBytes.length)
    buf.put(refBytes)
    buf.putLong(metadata.id)
    buf.putLong(metadata.parentId)
    buf.putLong(metadata.seq)
    buf.putLong(metadata.createdAt)
    buf.putLong(metadata.schemaVersion)
    buf.array()

  private def decodeMetadata(bytes: Array[Byte]): Snapshot.Metadata =
    val buf = ByteBuffer.wrap(bytes)
    check(buf.remaining() >= LengthSize, "truncated metadata")
    val refLength = buf.getInt()
    check(refLength > 0 && buf.remaining() >= refLength + MetadataLongFields * LongSize, "truncated metadata")
    val refBytes = new Array[Byte](refLength)
    buf.get(refBytes)
    Snapshot.Metadata(
      processRef = ProcessRef[Event](new String(refBytes, UTF_8)),
      id = buf.getLong(),
      parentId = buf.getLong(),
      seq = buf.getLong(),
      createdAt = buf.getLong(),
      schemaVersion = buf.getLong()
    )

  /** On-disk size of one `[length][payload][crc32]` section. */
  private def sectionSize(payloadLength: Int): Int =
    LengthSize + payloadLength + ChecksumSize

  // getValue returns Long only because Java lacks an unsigned int; the checksum itself is 32 bits
  private def crc32(bytes: Array[Byte]): Int =
    val crc = new CRC32()
    crc.update(bytes)
    crc.getValue.toInt

  private def check(condition: Boolean, message: => String): Unit =
    if !condition then throw new CorruptSnapshotException(message)
