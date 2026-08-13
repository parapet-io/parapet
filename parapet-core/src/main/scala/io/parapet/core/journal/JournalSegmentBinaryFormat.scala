package io.parapet.core.journal

import java.nio.ByteBuffer
import java.util.zip.CRC32

/** On-disk byte layout of a [[JournalSegment]]: a fixed-size, CRC-guarded metadata header followed by the entry blobs
  * (the [[JournalEntryBinaryFormat]] batch encoding).
  * {{{
  * header (fixed [[MetadataBytes]] bytes, big-endian):
  *   [magic "PJSG": Int][format version: Short]
  *   [minSeq: Long][maxSeq: Long][entryCount: Int][maxEnvelopeId: Long][createdAt: Long]
  *   [crc32(fields): Int]
  * then: entry blobs (JournalEntryBinaryFormat.encodeBatch)
  * }}}
  */
object JournalSegmentBinaryFormat:

  /** A segment header that cannot be decoded. */
  final class CorruptSegmentException(message: String) extends RuntimeException(message)

  private val Magic: Int     = 0x504a5347 // "PJSG"
  private val Version: Short = 1

  private val IntSize   = java.lang.Integer.BYTES
  private val ShortSize = java.lang.Short.BYTES
  private val LongSize  = java.lang.Long.BYTES

  // minSeq + maxSeq + entryCount + maxEnvelopeId + createdAt
  private val FieldsSize = LongSize + LongSize + IntSize + LongSize + LongSize

  /** Fixed size of the segment header - the only bytes a reader needs to obtain the [[JournalMetadata]]. */
  val MetadataBytes: Int = IntSize + ShortSize + FieldsSize + IntSize

  /** Encodes a segment: metadata header followed by the entry blobs. */
  def encode(segment: JournalSegment): Array[Byte] =
    val fields  = encodeFields(segment.metadata)
    val entries = JournalEntryBinaryFormat.encodeBatch(segment.entries)
    val buf     = ByteBuffer.allocate(MetadataBytes + entries.length)
    buf.putInt(Magic)
    buf.putShort(Version)
    buf.put(fields)
    buf.putInt(crc32(fields))
    buf.put(entries)
    buf.array()

  /** Decodes just the header from the leading bytes of a segment - `bytes` need only be at least [[MetadataBytes]]. */
  def readMetadata(bytes: Array[Byte]): JournalMetadata =
    check(bytes.length >= MetadataBytes, "truncated segment header")
    val buf = ByteBuffer.wrap(bytes)
    check(buf.getInt() == Magic, "not a journal segment (bad magic)")
    val version = buf.getShort()
    check(version == Version, s"unsupported segment format version: $version")
    val fields = new Array[Byte](FieldsSize)
    buf.get(fields)
    check(crc32(fields) == buf.getInt(), "segment header checksum mismatch")
    val f = ByteBuffer.wrap(fields)
    JournalMetadata(f.getLong(), f.getLong(), f.getInt(), f.getLong(), f.getLong())

  /** Decodes a full segment blob, cross-checking the header's `entryCount` against the decoded entries. */
  def decode(bytes: Array[Byte]): JournalSegment =
    val metadata = readMetadata(bytes)
    val entries = JournalEntryBinaryFormat.decodeBatch(java.util.Arrays.copyOfRange(bytes, MetadataBytes, bytes.length))
    check(
      entries.size == metadata.entryCount,
      s"entry count mismatch: header says ${metadata.entryCount}, decoded ${entries.size}"
    )
    JournalSegment(metadata, entries)

  private def encodeFields(m: JournalMetadata): Array[Byte] =
    val buf = ByteBuffer.allocate(FieldsSize)
    buf.putLong(m.minSeq)
    buf.putLong(m.maxSeq)
    buf.putInt(m.entryCount)
    buf.putLong(m.maxEnvelopeId)
    buf.putLong(m.createdAt)
    buf.array()

  private def crc32(bytes: Array[Byte]): Int =
    val crc = new CRC32()
    crc.update(bytes)
    crc.getValue.toInt

  private def check(condition: Boolean, message: => String): Unit =
    if !condition then throw new CorruptSegmentException(message)
