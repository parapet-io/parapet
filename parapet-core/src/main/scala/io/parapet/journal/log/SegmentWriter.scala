package io.parapet.journal.log

import io.parapet.Codec
import io.parapet.effect.Clock
import io.parapet.journal.FileUtils
import io.parapet.journal.log.SegmentFormat.{FooterMagic, FormatVersion, HeaderMagic, encodeFrame, trailer}

import java.nio.ByteBuffer
import java.nio.channels.FileChannel

trait SegmentWriter[Entry, Header, Footer] {

  def writeHeader(header: Header): Unit

  /** Writes an entry frame, as [[SegmentFormat.encodeFrame]] builds it. */
  def writeEntry(frame: Array[Byte]): Unit
  def writeFooter(footer: Footer): Unit
  def segmentId: Long
  def size: Long
  def entryCount: Long
  def createdAt: Long
  def closedAt: Long
  def isOpen: Boolean
  def force(): Unit
  def close(): Unit

}

object SegmentWriter {

  def apply[Entry, Header, Footer](
      channel: FileChannel,
      segmentId: Long,
      codec: LogCodec[Entry, Header, Footer],
      clock: Clock = Clock()
  ): SegmentWriter[Entry, Header, Footer] = Impl(channel, segmentId, clock.currentTimeMillis, codec, clock)

  /** A writer for a segment that already holds its header and `entryCount` entries. `channel` must be positioned at
    * `size`, where they end.
    */
  def resumeOpen[Entry, Header, Footer](
      channel: FileChannel,
      segmentId: Long,
      createdAt: Long,
      entryCount: Long,
      size: Long,
      codec: LogCodec[Entry, Header, Footer],
      clock: Clock = Clock()
  ): SegmentWriter[Entry, Header, Footer] = {
    val writer = Impl(channel, segmentId, createdAt, codec, clock)
    writer._entryCount = entryCount
    writer._size = size
    writer
  }

  class Impl[Entry, Header, Footer](
      channel: FileChannel,
      val segmentId: Long,
      val createdAt: Long,
      codec: LogCodec[Entry, Header, Footer],
      clock: Clock = Clock()
  ) extends SegmentWriter[Entry, Header, Footer] {

    private given Codec[Header] = codec.headerCodec
    private given Codec[Footer] = codec.footerCodec
    private val headerCodec     = summon[Codec[SegmentHeader[Header]]]
    private val footerCodec     = summon[Codec[SegmentFooter[Footer]]]

    private[SegmentWriter] var _size       = 0L
    private[SegmentWriter] var _entryCount = 0L
    private var _closedAt                  = 0L
    private var _closed                    = false

    override def writeHeader(header: Header): Unit = {
      val segHeader = SegmentHeader(FormatVersion, segmentId, codec.version, createdAt, header)
      write(HeaderMagic, segHeader, headerCodec)
    }

    override def writeEntry(frame: Array[Byte]): Unit = {
      write(frame)
      _entryCount += 1
    }

    override def writeFooter(footer: Footer): Unit = {
      val segFooter = SegmentFooter(_entryCount, codec.version, createdAt, clock.currentTimeMillis, footer)
      val size      = write(FooterMagic, segFooter, footerCodec)
      write(trailer(size))
    }

    override def size: Long = _size

    override def entryCount: Long = _entryCount

    override def force(): Unit =
      channel.force(true)

    override def isOpen: Boolean = !_closed

    override def closedAt: Long = _closedAt

    override def close(): Unit =
      if (!_closed) {
        channel.close()
        _closed = true
        _closedAt = clock.currentTimeMillis
      }

    private def write[A](magic: Int, value: A, codec: Codec[A]): Int = {
      val bytes = encodeFrame(magic, value)(using codec)
      write(bytes)
      bytes.length
    }

    private def write(bytes: Array[Byte]): Unit = {
      FileUtils.writeFully(channel, ByteBuffer.wrap(bytes))
      _size += bytes.length
    }
  }

}
