package io.parapet.journal.log

import io.parapet.Codec
import io.parapet.journal.log.FrameReader.{Error, Frame}
import io.parapet.journal.log.SegmentFormat.*
import io.parapet.journal.log.SegmentReader.*

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import scala.util.{Failure, Success}

/** Reads a sealed segment: its header and footer when it is opened, then its entries in order.
  *
  * A sealed file ends with a trailer that says where its footer starts, and its entries end there. A segment is sealed
  * only once it is complete, so any damage in one is corruption: a CorruptLogException, an InvalidChecksumException for
  * a failed checksum. An unknown storage format or codec version throws UnsupportedLogVersionException. After an
  * exception the reader must not be used again. Whether the footer agrees with the header and the entries is for the
  * caller to check. The reader sees the file as it is when it is opened. The caller owns the channel.
  *
  * The open file is not read with this reader. A crash can cut that file anywhere, so its last bytes prove nothing:
  * what looks like a trailer there may lie inside an entry. Recovery reads it frame by frame from the start instead.
  */
final class SegmentReader[E, Header, Footer] private (
    frames: FrameReader,
    codec: LogCodec[E, Header, Footer],
    val header: SegmentHeader[Header],
    val footer: SegmentFooter[Footer],
    entriesEnd: Long
) {

  /** Whether an entry remains before the footer. */
  def hasEntry: Boolean = frames.position < entriesEnd

  /** Reads the next entry. Throws NoSuchElementException unless [[hasEntry]]. */
  def readEntry(): E = {
    if (!hasEntry) throw new NoSuchElementException("the segment has no entry left")
    val start = frames.position
    val frame = readFrame(frames)
    if (frame.kind != FrameKind.Entry) throw corrupt(start, s"unexpected ${frame.kind} frame")
    if (frames.position > entriesEnd) throw corrupt(start, "the entry runs into the footer")
    decodeEntry(start, frame.data, codec)
  }
}

object SegmentReader {

  /** Opens the sealed segment behind `channel`, whatever the channel's position. */
  def open[E, Header, Footer](
      channel: SeekableByteChannel,
      codecs: Int => Option[LogCodec[E, Header, Footer]]
  ): SegmentReader[E, Header, Footer] = {
    val frames                = FrameReader(channel)
    val (header, codec)       = decodeHeader(readFrame(frames), codecs)
    val entriesStart          = frames.position
    val (footerStart, footer) = findFooter(frames, codecs)
    frames.position(entriesStart)
    new SegmentReader(frames, codec, header, footer, footerStart)
  }

  /** Reads only the header of the sealed segment behind `channel`, whatever the channel's position. */
  def readHeader[E, Header, Footer](
      channel: SeekableByteChannel,
      codecs: Int => Option[LogCodec[E, Header, Footer]]
  ): SegmentHeader[Header] = {
    val (header, _) = decodeHeader(readFrame(FrameReader(channel)), codecs)
    header
  }

  /** Reads only the footer of the sealed segment behind `channel`, whatever the channel's position. The footer names
    * its own codec version, so nothing before it is read.
    */
  def readFooter[E, Header, Footer](
      channel: SeekableByteChannel,
      codecs: Int => Option[LogCodec[E, Header, Footer]]
  ): SegmentFooter[Footer] = {
    val (_, footer) = findFooter(FrameReader(channel), codecs)
    footer
  }

  /** The footer that the trailer points to, and where its frame starts. */
  private def findFooter[E, Header, Footer](
      frames: FrameReader,
      codecs: Int => Option[LogCodec[E, Header, Footer]]
  ): (Long, SegmentFooter[Footer]) = {
    val start = frames.readTrailer().getOrElse(throw corrupt(frames.size, "the segment has no trailer"))
    frames.position(start)
    val frame = readFrame(frames)
    // The trailer has no checksum. The frame it points to vouches for it: a footer that ends where the trailer starts.
    if (frame.kind != FrameKind.Footer || frames.position != frames.size - TrailerSize)
      throw corrupt(start, "the trailer does not point to the footer")
    (start, decodeFooter(start, frame.data, codecs))
  }

  /** The codecs of `version`, which must be the version they report. */
  private[log] def lookup[E, Header, Footer](
      codecs: Int => Option[LogCodec[E, Header, Footer]],
      version: Int
  ): LogCodec[E, Header, Footer] = {
    val codec =
      codecs(version).getOrElse(throw new UnsupportedLogVersionException(s"unsupported codec version: $version"))
    if (codec.version != version)
      throw new IllegalStateException(s"the codecs for version $version report version ${codec.version}")
    codec
  }

  /** The header in the first frame of a file, and the codecs of the version it names, which decode the rest. */
  private[log] def decodeHeader[E, Header, Footer](
      frame: Frame,
      codecs: Int => Option[LogCodec[E, Header, Footer]]
  ): (SegmentHeader[Header], LogCodec[E, Header, Footer]) = {
    if (frame.kind != FrameKind.Header) throw corrupt(0L, s"the file starts with a ${frame.kind} frame")
    val payload = ByteBuffer.wrap(frame.data)
    if (payload.remaining() < VersionSize) throw corrupt(0L, "missing storage format version")
    // This first field and the frame envelope are stable across storage versions.
    val formatVersion = payload.getInt()
    if (formatVersion != FormatVersion)
      throw new UnsupportedLogVersionException(s"unsupported storage format version: $formatVersion")
    if (payload.remaining() < SegmentHeaderFixedSize - VersionSize) throw corrupt(0L, "incomplete segment header")
    val segmentId = payload.getLong()
    val version   = payload.getInt()
    val createdAt = payload.getLong()
    if (segmentId < 0) throw corrupt(0L, s"negative segment ID: $segmentId")
    val codec  = lookup(codecs, version)
    val header = decode(0L, FrameKind.Header, payload, codec.headerCodec)
    (SegmentHeader(formatVersion, segmentId, version, createdAt, header), codec)
  }

  /** The entry in the payload of the entry frame that starts at `start`. */
  private[log] def decodeEntry[E, Header, Footer](
      start: Long,
      data: Array[Byte],
      codec: LogCodec[E, Header, Footer]
  ): E =
    decode(start, FrameKind.Entry, ByteBuffer.wrap(data), codec.entryCodec)

  /** The footer in the payload of the footer frame that starts at `start`. */
  private[log] def decodeFooter[E, Header, Footer](
      start: Long,
      data: Array[Byte],
      codecs: Int => Option[LogCodec[E, Header, Footer]]
  ): SegmentFooter[Footer] = {
    val payload = ByteBuffer.wrap(data)
    if (payload.remaining() < SegmentFooterFixedSize) throw corrupt(start, "incomplete segment footer")
    val entryCount   = payload.getLong()
    val codecVersion = payload.getInt()
    val createdAt    = payload.getLong()
    val sealedAt     = payload.getLong()
    if (entryCount <= 0) throw corrupt(start, s"the footer entry count must be positive: $entryCount")
    // Wall-clock adjustments may make sealedAt earlier than createdAt.
    val footer = decode(start, FrameKind.Footer, payload, lookup(codecs, codecVersion).footerCodec)
    SegmentFooter(entryCount, codecVersion, createdAt, sealedAt, footer)
  }

  /** The frame at the position of a sealed segment, where damage is corruption. */
  private def readFrame(frames: FrameReader): Frame = {
    val start = frames.position
    frames.read() match {
      case Right(frame)                => frame
      case Left(Error.Incomplete)      => throw corrupt(start, "the segment ends inside a frame")
      case Left(Error.InvalidChecksum) => throw new InvalidChecksumException(s"at offset $start: invalid checksum")
    }
  }

  /** Decodes one value from `payload`, starting at its position; nothing may follow the value. `start` is where the
    * frame of `kind` that holds it starts.
    */
  private def decode[A](start: Long, kind: FrameKind, payload: ByteBuffer, codec: Codec[A]): A = {
    val value = codec.decode(payload) match {
      case Success(value) => value
      case Failure(error) => throw new CorruptLogException(s"at offset $start: invalid $kind payload", error)
    }
    if (payload.hasRemaining) throw corrupt(start, s"unexpected bytes after the $kind payload")
    value
  }

  private def corrupt(offset: Long, message: String): CorruptLogException =
    new CorruptLogException(s"at offset $offset: $message")
}
