package io.parapet.journal

import io.parapet.{Event, ProcessRef}
import io.parapet.journal.DeliveryLog.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Path
import java.util.zip.CRC32C

class DeliveryLogReaderSpec extends AnyFunSuite:

  private val source    = Path.of("memory.jrnl")
  private val header    = Header(firstSeq = 11L, createdAtMillis = 100L)
  private val highWater = HighWater(maxSeq = 17L, maxEnvelopeId = 41L)
  private val scanner   = new DeliveryLog(
    Config(Path.of("unused"), maxSegmentBytes = 1024L * 1024L, maxEntryBytes = 1024 * 1024)
  )
  private val entry = JournalEntry(
    seq = 11L,
    id = 41L,
    sender = ProcessRef[Event]("sender"),
    receiver = ProcessRef[Event]("receiver"),
    cause = 7L,
    event = "event".getBytes(UTF_8),
    tag = "event",
    schemaVersion = 1
  )
  private val footer = Footer(
    firstSeq = 11L,
    lastSeq = 13L,
    entryCount = 3L,
    entriesEndOffset = Header.EncodedSize.toLong,
    createdAtMillis = 100L,
    sealedAtMillis = 200L,
    maxEnvelopeId = 41L
  )

  private val headerBytes    = encodeHeaderFixture(header)
  private val highWaterBytes = encodeHighWaterFixture(highWater)
  private val entryBytes     = encodeFrameFixture(entry)
  private val sealedFooter   = footer.copy(
    lastSeq = entry.seq,
    entryCount = 1L,
    entriesEndOffset = Header.EncodedSize.toLong + entryBytes.length,
    maxEnvelopeId = math.max(entry.id, entry.cause)
  )

  private def segmentBytes(value: Footer = footer): Array[Byte] =
    headerBytes ++ encodeFooterFixture(value)

  private def channel(bytes: Array[Byte], maxReadSize: Int = Int.MaxValue): SeekableByteChannel =
    new InMemorySeekableByteChannel(bytes, maxReadSize)

  private def mutate(bytes: Array[Byte])(change: Array[Byte] => Unit): Array[Byte] =
    val changed = bytes.clone()
    change(changed)
    changed

  private def encodeHeaderFixture(value: Header): Array[Byte] =
    val fields = ByteBuffer
      .allocate(Header.WithoutChecksumSize)
      .putInt(Header.Magic)
      .putShort(Header.FormatVersion)
      .putLong(value.firstSeq)
      .putLong(value.createdAtMillis)
      .array()
    ByteBuffer.allocate(Header.EncodedSize).put(fields).putInt(checksum(fields)).array()

  private def encodeHighWaterFixture(value: HighWater): Array[Byte] =
    val fields = ByteBuffer
      .allocate(HighWater.WithoutChecksumSize)
      .putInt(HighWater.Magic)
      .putShort(HighWater.FormatVersion)
      .putLong(value.maxSeq)
      .putLong(value.maxEnvelopeId)
      .array()
    ByteBuffer.allocate(HighWater.EncodedSize).put(fields).putInt(checksum(fields)).array()

  private def encodeFooterFixture(value: Footer): Array[Byte] =
    val payload = ByteBuffer
      .allocate(Footer.PayloadSize)
      .putLong(value.firstSeq)
      .putLong(value.lastSeq)
      .putLong(value.entryCount)
      .putLong(value.entriesEndOffset)
      .putLong(value.createdAtMillis)
      .putLong(value.sealedAtMillis)
      .putLong(value.maxEnvelopeId)
      .array()
    ByteBuffer
      .allocate(Footer.EncodedSize)
      .putInt(Footer.StartMagic)
      .put(payload)
      .putInt(checksum(payload))
      .putInt(Footer.EndMagic)
      .array()

  private def encodeFrameFixture(value: JournalEntry): Array[Byte] =
    val payload       = JournalEntryBinaryFormat.encode(value)
    val checksumInput = ByteBuffer
      .allocate(EntryFrame.checksumInputSize(payload.length))
      .putLong(value.seq)
      .putInt(payload.length)
      .put(payload)
      .array()
    ByteBuffer
      .allocate(EntryFrame.encodedSize(payload.length))
      .putInt(EntryFrame.Magic)
      .putLong(value.seq)
      .putInt(payload.length)
      .put(payload)
      .putInt(checksum(checksumInput))
      .array()

  private def checksum(bytes: Array[Byte]): Int =
    val crc = new CRC32C()
    crc.update(bytes)
    crc.getValue.toInt

  test("readHighWater reads a valid checkpoint across partial channel reads") {
    readHighWater(source, channel(highWaterBytes, maxReadSize = 3)) shouldBe highWater
  }

  test("readHighWater rejects a truncated checkpoint") {
    val error = the[CorruptLogException] thrownBy
      readHighWater(source, channel(highWaterBytes.dropRight(1)))

    error.getMessage should include("high-water file length")
  }

  test("readHighWater rejects bytes after the checkpoint") {
    val error = the[CorruptLogException] thrownBy
      readHighWater(source, channel(highWaterBytes :+ 0.toByte))

    error.getMessage should include("high-water file length")
  }

  test("readHighWater rejects invalid magic") {
    val bytes = mutate(highWaterBytes)(bytes => ByteBuffer.wrap(bytes).putInt(0, 0))

    val error = the[CorruptLogException] thrownBy readHighWater(source, channel(bytes))
    error.getMessage should include("high-water magic")
  }

  test("readHighWater rejects an unsupported format version") {
    val bytes = mutate(highWaterBytes) { bytes =>
      ByteBuffer.wrap(bytes).putShort(java.lang.Integer.BYTES, (HighWater.FormatVersion + 1).toShort)
    }

    val error = the[CorruptLogException] thrownBy readHighWater(source, channel(bytes))
    error.getMessage should include("unsupported delivery high-water version")
  }

  test("readHighWater rejects a checksum mismatch") {
    val bytes = mutate(highWaterBytes) { bytes =>
      val index = HighWater.WithoutChecksumSize - 1
      bytes(index) = (bytes(index) ^ 1).toByte
    }

    val error = the[CorruptLogException] thrownBy readHighWater(source, channel(bytes))
    error.getMessage should include("high-water checksum")
  }

  test("readHighWater rejects negative values") {
    val bytes = encodeHighWaterFixture(highWater.copy(maxSeq = -1L))

    val error = the[CorruptLogException] thrownBy readHighWater(source, channel(bytes))
    error.getMessage should include("negative delivery high-water sequence")
  }

  test("readHeader reads a valid header across partial channel reads") {
    readHeader(source, channel(headerBytes, maxReadSize = 3)) shouldBe header
  }

  test("readHeader rejects a truncated header") {
    val error = the[CorruptLogException] thrownBy
      readHeader(source, channel(headerBytes.dropRight(1)))

    error.getMessage should include("truncated")
  }

  test("readHeader rejects invalid magic") {
    val bytes = mutate(headerBytes)(bytes => ByteBuffer.wrap(bytes).putInt(0, 0))

    val error = the[CorruptLogException] thrownBy readHeader(source, channel(bytes))
    error.getMessage should include("header magic")
  }

  test("readHeader rejects an unsupported format version") {
    val bytes = mutate(headerBytes) { bytes =>
      ByteBuffer.wrap(bytes).putShort(java.lang.Integer.BYTES, (Header.FormatVersion + 1).toShort)
    }

    val error = the[CorruptLogException] thrownBy readHeader(source, channel(bytes))
    error.getMessage should include("unsupported delivery-log version")
  }

  test("readHeader rejects a checksum mismatch") {
    val bytes = mutate(headerBytes) { bytes =>
      val index = Header.WithoutChecksumSize - 1
      bytes(index) = (bytes(index) ^ 1).toByte
    }

    val error = the[CorruptLogException] thrownBy readHeader(source, channel(bytes))
    error.getMessage should include("header checksum")
  }

  test("readFooter reads a valid footer across partial channel reads") {
    readFooter(source, channel(segmentBytes(), maxReadSize = 3)) shouldBe footer
  }

  test("readFooter rejects a channel shorter than the trailer") {
    val error = the[CorruptLogException] thrownBy
      readFooter(source, channel(Array.ofDim[Byte](Footer.TrailerSize - 1)))

    error.getMessage should include("too short to contain a footer trailer")
  }

  test("readFooter rejects invalid end magic") {
    val bytes = mutate(segmentBytes()) { bytes =>
      val index = bytes.length - 1
      bytes(index) = (bytes(index) ^ 1).toByte
    }

    val error = the[CorruptLogException] thrownBy readFooter(source, channel(bytes))
    error.getMessage should include("footer end magic")
  }

  test("readFooter rejects a footer that starts before the entry region") {
    val bytes = encodeFooterFixture(footer.copy(entriesEndOffset = 0L))

    val error = the[CorruptLogException] thrownBy readFooter(source, channel(bytes))
    error.getMessage should include("footer starts before")
  }

  test("readFooter rejects invalid start magic") {
    val bytes = mutate(segmentBytes()) { bytes =>
      ByteBuffer.wrap(bytes).putInt(Header.EncodedSize, 0)
    }

    val error = the[CorruptLogException] thrownBy readFooter(source, channel(bytes))
    error.getMessage should include("footer start magic")
  }

  test("readFooter rejects a checksum mismatch") {
    val bytes = mutate(segmentBytes()) { bytes =>
      val payloadStart = Header.EncodedSize + MagicSize
      bytes(payloadStart) = (bytes(payloadStart) ^ 1).toByte
    }

    val error = the[CorruptLogException] thrownBy readFooter(source, channel(bytes))
    error.getMessage should include("footer checksum")
  }

  test("readFooter rejects an entries end offset that differs from the footer position") {
    val bytes = segmentBytes(footer.copy(entriesEndOffset = footer.entriesEndOffset + 1L))

    val error = the[CorruptLogException] thrownBy readFooter(source, channel(bytes))
    error.getMessage should include("footer entries end offset")
  }

  test("scanSegment identifies an open segment") {
    val bytes   = headerBytes ++ entryBytes
    val segment = scanner.scanSegment(source, channel(bytes, maxReadSize = 3))

    segment.entries.map(_.seq) shouldBe Vector(entry.seq)
    segment.metadata.entriesEndOffset shouldBe bytes.length.toLong
    segment.metadata.state shouldBe SegmentState.Open
  }

  test("scanSegment identifies and validates a sealed segment") {
    val bytes   = headerBytes ++ entryBytes ++ encodeFooterFixture(sealedFooter)
    val segment = scanner.scanSegment(source, channel(bytes, maxReadSize = 3))

    segment.entries.map(_.seq) shouldBe Vector(entry.seq)
    segment.metadata.entriesEndOffset shouldBe sealedFooter.entriesEndOffset
    segment.metadata.state shouldBe SegmentState.Sealed(sealedFooter.sealedAtMillis)
  }

  test("scanSegment reports an interrupted footer as open") {
    val dataBytes    = headerBytes ++ entryBytes
    val footerPrefix = encodeFooterFixture(sealedFooter).take(MagicSize + java.lang.Long.BYTES)
    val segment      = scanner.scanSegment(source, channel(dataBytes ++ footerPrefix))

    segment.entries.map(_.seq) shouldBe Vector(entry.seq)
    segment.metadata.entriesEndOffset shouldBe dataBytes.length.toLong
    segment.metadata.state shouldBe SegmentState.Open
  }

  test("scanSegment reports an incomplete entry as open at the last valid boundary") {
    val segment = scanner.scanSegment(source, channel(headerBytes ++ entryBytes.take(MagicSize + 1)))

    segment.entries shouldBe empty
    segment.metadata.entriesEndOffset shouldBe Header.EncodedSize.toLong
    segment.metadata.state shouldBe SegmentState.Open
  }

  test("scanSegment handles a large incomplete payload without Int frame-size overflow") {
    val frameHeader = ByteBuffer
      .allocate(EntryFrame.HeaderSize)
      .putInt(EntryFrame.Magic)
      .putLong(entry.seq)
      .putInt(Int.MaxValue)
      .array()
    val segment = scanner.scanSegment(source, channel(headerBytes ++ frameHeader))

    segment.entries shouldBe empty
    segment.metadata.entriesEndOffset shouldBe Header.EncodedSize.toLong
    segment.metadata.state shouldBe SegmentState.Open
  }

  test("scanSegment rejects bytes after a complete footer") {
    val bytes = (headerBytes ++ entryBytes ++ encodeFooterFixture(sealedFooter)) :+ 0.toByte

    val error = the[CorruptLogException] thrownBy scanner.scanSegment(source, channel(bytes))
    error.getMessage should include("unexpected bytes after footer")
  }
