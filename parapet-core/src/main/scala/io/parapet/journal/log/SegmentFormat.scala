package io.parapet.journal.log

import io.parapet.Codec

import java.nio.ByteBuffer
import java.util.zip.CRC32C

/** The byte layout of a segment file: a header frame, entry frames and, once sealed, a footer frame and a trailer. */
object SegmentFormat {
  private[log] val FormatVersion = 1
  private[log] val HeaderMagic   = 0x504c4844
  private[log] val EntryMagic    = 0x504c5243
  private[log] val FooterMagic   = 0x504c4654
  private[log] val EndMagic      = 0x504c454e

  private[log] val MagicSize       = Integer.BYTES
  private[log] val LengthSize      = Integer.BYTES
  private[log] val VersionSize     = Integer.BYTES
  private[log] val ChecksumSize    = Integer.BYTES
  private[log] val TrailerSize     = LengthSize + MagicSize
  private[log] val FrameHeaderSize = LengthSize + MagicSize + ChecksumSize // [magic:4][payload length:4][header CRC:4]
  private[log] val FrameOverhead   = FrameHeaderSize + ChecksumSize
  private[log] val SegmentHeaderFixedSize = VersionSize + java.lang.Long.BYTES + VersionSize + java.lang.Long.BYTES
  private[log] val SegmentFooterFixedSize = java.lang.Long.BYTES * 3 + VersionSize

  enum FrameKind(val magic: Int):
    case Header extends FrameKind(HeaderMagic)
    case Entry  extends FrameKind(EntryMagic)
    case Footer extends FrameKind(FooterMagic)

  /** A frame header whose checksum matches. */
  final private[log] case class FrameHeader(kind: FrameKind, payloadLength: Int)

  private[log] def checksum(bytes: Array[Byte]): Int                           = checksum(bytes, 0, bytes.length)
  private[log] def checksum(bytes: Array[Byte], offset: Int, length: Int): Int = {
    val crc = new CRC32C
    crc.update(bytes, offset, length)
    crc.getValue.toInt
  }

  private[log] def trailer(footerFrameLength: Int): Array[Byte] =
    ByteBuffer.allocate(TrailerSize).putInt(footerFrameLength).putInt(EndMagic).array()

  // [magic:4][payload length:4][header CRC:4][payload:N][payload CRC:4]
  // The payload must not be empty: the reader rejects a zero length as corruption.
  private[parapet] def encodeFrame[A: Codec](magic: Int, payload: A): Array[Byte] = {
    val bytes = summon[Codec[A]].encode(payload).get
    require(bytes.nonEmpty, "a frame payload must not be empty")
    val buffer = ByteBuffer.allocate(Math.addExact(FrameOverhead, bytes.length))
    buffer.putInt(magic).putInt(bytes.length)
    putChecksum(buffer, 0, MagicSize + LengthSize)
    buffer.put(bytes)
    putChecksum(buffer, FrameHeaderSize, bytes.length)
    buffer.array()
  }

  private def putChecksum(buffer: ByteBuffer, offset: Int, length: Int): Unit =
    buffer.putInt(checksum(buffer.array(), buffer.arrayOffset() + offset, length))
}
