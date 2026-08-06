package io.parapet.core.journal

import io.parapet.{Event, ProcessRef}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util.zip.CRC32

/** The on-disk byte layout of a [[JournalEntry]] and of a batch of them.
  *
  * A batch file is simply the concatenation of self-contained per-entry blobs, so entries are read back one after
  * another. Each blob is CRC-guarded (big-endian):
  * {{{
  * [magic "PJRN": Int][format version: Short]
  * [payload length: Int][payload][crc32(payload): Int]
  *
  * payload = [seq: Long]
  *           [id: Long]
  *           [sender length: Int][sender bytes]
  *           [receiver length: Int][receiver bytes]
  *           [cause: Long]
  *           [event length: Int][event bytes]
  * }}}
  */
object JournalEntryBinaryFormat:

  /** A blob that cannot be decoded. */
  final class CorruptEntryException(message: String) extends RuntimeException(message)

  private val Magic: Int           = 0x504a524e // "PJRN"
  private val FormatVersion: Short = 2

  private val MagicSize    = java.lang.Integer.BYTES
  private val VersionSize  = java.lang.Short.BYTES
  private val IntSize      = java.lang.Integer.BYTES
  private val LongSize     = java.lang.Long.BYTES
  private val ChecksumSize = java.lang.Integer.BYTES
  private val HeaderSize   = MagicSize + VersionSize

  /** Encodes one entry to a self-contained blob. */
  def encode(entry: JournalEntry): Array[Byte] =
    val payload = encodePayload(entry)
    val buf     = ByteBuffer.allocate(HeaderSize + IntSize + payload.length + ChecksumSize)
    buf.putInt(Magic)
    buf.putShort(FormatVersion)
    buf.putInt(payload.length)
    buf.put(payload)
    buf.putInt(crc32(payload))
    buf.array()

  /** Encodes a batch as the concatenation of per-entry blobs. */
  def encodeBatch(entries: Seq[JournalEntry]): Array[Byte] =
    val parts = entries.map(encode)
    val out   = ByteBuffer.allocate(parts.foldLeft(0)(_ + _.length))
    parts.foreach(out.put)
    out.array()

  /** Decodes a single-entry blob. */
  def decode(bytes: Array[Byte]): JournalEntry =
    val buf   = ByteBuffer.wrap(bytes)
    val entry = readEntry(buf)
    check(!buf.hasRemaining, "trailing bytes after entry")
    entry

  /** Decodes every entry in a batch blob, in stored order. */
  def decodeBatch(bytes: Array[Byte]): Vector[JournalEntry] =
    val buf     = ByteBuffer.wrap(bytes)
    val entries = Vector.newBuilder[JournalEntry]
    while buf.hasRemaining do entries += readEntry(buf)
    entries.result()

  private def readEntry(buf: ByteBuffer): JournalEntry =
    check(buf.remaining() >= HeaderSize, "truncated header")
    check(buf.getInt() == Magic, "not a journal entry (bad magic)")
    val version = buf.getShort()
    check(version == FormatVersion, s"unsupported format version: $version")
    val payload = readBytes(buf, "payload")
    check(crc32(payload) == readInt(buf, "payload checksum"), "payload checksum mismatch")
    decodePayload(payload)

  private def encodePayload(entry: JournalEntry): Array[Byte] =
    val sender   = entry.sender.value.getBytes(UTF_8)
    val receiver = entry.receiver.value.getBytes(UTF_8)
    val buf      = ByteBuffer.allocate(
      LongSize + LongSize + (IntSize + sender.length) + (IntSize + receiver.length) + LongSize +
        (IntSize + entry.event.length)
    )
    buf.putLong(entry.seq)
    buf.putLong(entry.id)
    buf.putInt(sender.length)
    buf.put(sender)
    buf.putInt(receiver.length)
    buf.put(receiver)
    buf.putLong(entry.cause)
    buf.putInt(entry.event.length)
    buf.put(entry.event)
    buf.array()

  private def decodePayload(bytes: Array[Byte]): JournalEntry =
    val buf      = ByteBuffer.wrap(bytes)
    val seq      = readLong(buf, "seq")
    val id       = readLong(buf, "id")
    val sender   = readRef(readBytes(buf, "sender"), "sender")
    val receiver = readRef(readBytes(buf, "receiver"), "receiver")
    val cause    = readLong(buf, "cause")
    val event    = readBytes(buf, "event")
    JournalEntry(seq, id, sender, receiver, cause, event)

  private def readRef(bytes: Array[Byte], name: String): ProcessRef.Unknown =
    check(bytes.nonEmpty, s"empty $name ref")
    ProcessRef[Event](new String(bytes, UTF_8))

  /** Reads a `[length: Int][bytes]` chunk, bounds-checking the length before allocating. */
  private def readBytes(buf: ByteBuffer, name: String): Array[Byte] =
    val length = readInt(buf, s"$name length")
    check(length >= 0 && length <= buf.remaining(), s"truncated $name")
    val bytes = new Array[Byte](length)
    buf.get(bytes)
    bytes

  private def readInt(buf: ByteBuffer, name: String): Int =
    check(buf.remaining() >= IntSize, s"truncated $name")
    buf.getInt()

  private def readLong(buf: ByteBuffer, name: String): Long =
    check(buf.remaining() >= LongSize, s"truncated $name")
    buf.getLong()

  // getValue returns Long only because Java lacks an unsigned int; the checksum itself is 32 bits
  private def crc32(bytes: Array[Byte]): Int =
    val crc = new CRC32()
    crc.update(bytes)
    crc.getValue.toInt

  private def check(condition: Boolean, message: => String): Unit =
    if !condition then throw new CorruptEntryException(message)
