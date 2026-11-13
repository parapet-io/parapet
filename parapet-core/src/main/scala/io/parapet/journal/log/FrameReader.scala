package io.parapet.journal.log

import io.parapet.journal.log.FrameReader.*
import io.parapet.journal.log.SegmentFormat.*

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

/** Reads the frames of a segment file one at a time, and the trailer of a sealed one. It checks their lengths and
  * checksums, not their order or what their payloads hold.
  *
  * An Error is damage a crash can leave. It leaves the position at the damaged frame, so every byte before the position
  * is valid. Bytes a crash cannot produce throw CorruptLogException. The reader sees the file as it is when the reader
  * is created. The caller owns the channel.
  */
final class FrameReader(channel: SeekableByteChannel) {

  /** The size of the file when the reader was created. */
  val size: Long = channel.size()

  private var pos = 0L

  /** Where the next frame is read: at first the start of the file, then the end of the last frame read. */
  def position: Long = pos

  /** Moves to `pos`, which must be where a frame starts: any other bytes read as a damaged frame. */
  def position(pos: Long): Unit = {
    require(pos >= 0 && pos <= size, s"position $pos is outside the file of $size bytes")
    this.pos = pos
  }

  /** Reads the frame at the position and moves past it. The end of the file holds no frame, which is Incomplete like a
    * frame cut short, so a caller that expects the end reads only while `position < size`.
    */
  def read(): Either[Error, Frame] =
    for {
      header <- readHeader()
      data   <- readPayload(header.payloadLength)
    } yield {
      pos += FrameOverhead.toLong + data.length
      Frame(header.kind, data)
    }

  /** Where the trailer that ends the file says the footer frame starts, or None when the file does not end with a
    * trailer, as the open segment does not. The trailer has no checksum: its end magic and a footer frame that fits
    * before it are its only checks, so the caller checks the frame it points to. The position does not move.
    */
  def readTrailer(): Option[Long] = {
    val start = size - TrailerSize
    if (start < 0) None
    else {
      val bytes       = readBytes(start, TrailerSize)
      val footerStart = start - bytes.getInt()
      val endMagic    = bytes.getInt()
      Option.when(endMagic == EndMagic && footerStart >= 0 && start - footerStart > FrameOverhead)(footerStart)
    }
  }

  /** The frame header at the position. Its checksum covers the magic and the payload length, so neither is trusted
    * before it matches.
    */
  private def readHeader(): Either[Error, FrameHeader] =
    if (size - pos < FrameOverhead) Left(Error.Incomplete)
    else {
      val bytes  = readBytes(pos, FrameHeaderSize)
      val magic  = bytes.getInt()
      val length = bytes.getInt()
      if (bytes.getInt() != checksum(bytes.array(), 0, MagicSize + LengthSize)) Left(Error.InvalidChecksum)
      else {
        val kind = KindsByMagic.getOrElse(magic, throw corrupt(f"unknown frame magic 0x$magic%08x"))
        // Every frame has a payload: the writer rejects an empty one.
        if (length <= 0) throw corrupt(s"$kind payload length must be positive: $length")
        Right(FrameHeader(kind, length))
      }
    }

  /** The payload after the frame header at the position. Its length is checked against the file before the payload is
    * allocated.
    */
  private def readPayload(length: Int): Either[Error, Array[Byte]] =
    if (length > size - pos - FrameOverhead) Left(Error.Incomplete)
    else {
      val start = pos + FrameHeaderSize
      val data  = readBytes(start, length).array()
      if (readBytes(start + length, ChecksumSize).getInt() != checksum(data)) Left(Error.InvalidChecksum)
      else Right(data)
    }

  /** Reads exactly `length` bytes at `offset`; the caller has checked that they lie within the file. */
  private def readBytes(offset: Long, length: Int): ByteBuffer = {
    val buffer = ByteBuffer.allocate(length)
    channel.position(offset)
    while (buffer.hasRemaining) {
      val count = channel.read(buffer)
      if (count < 0)
        throw new CorruptLogException(
          s"the file ends at offset ${offset + buffer.position()}, before its size of $size bytes"
        )
      if (count == 0) throw new IOException("reading the segment made no progress")
    }
    buffer.flip()
    buffer
  }

  private def corrupt(message: String): CorruptLogException =
    new CorruptLogException(s"at offset $pos: $message")
}

object FrameReader {

  /** Damage a crash can leave in the frame at the reader's position. */
  enum Error:
    /** The file ends before the frame does. */
    case Incomplete

    /** The frame header or the payload does not match its checksum. */
    case InvalidChecksum

  /** A frame whose checksums match. `data` is its payload, without the framing bytes. */
  final case class Frame(kind: FrameKind, data: Array[Byte])

  private val KindsByMagic = FrameKind.values.map(kind => kind.magic -> kind).toMap
}
