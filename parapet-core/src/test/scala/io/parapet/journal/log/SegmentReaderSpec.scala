package io.parapet.journal.log

import io.parapet.Codec
import io.parapet.journal.InMemorySeekableByteChannel
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.ByteBuffer
import java.util.zip.CRC32C

class SegmentReaderSpec extends AnyFunSuite:
  private val headerMagic = 0x504c4844
  private val entryMagic  = 0x504c5243
  private val footerMagic = 0x504c4654
  private val endMagic    = 0x504c454e

  private type Reader = SegmentReader[Int, Int, Int]

  private val version1 = new LogCodec[Int, Int, Int]:
    val version     = 1
    val headerCodec = summon[Codec[Int]]
    val entryCodec  = summon[Codec[Int]]
    val footerCodec = summon[Codec[Int]]

  private def codecs(version: Int): Option[LogCodec[Int, Int, Int]] = Option.when(version == 1)(version1)

  private def open(bytes: Array[Byte], maxReadSize: Int = Int.MaxValue): Reader =
    SegmentReader.open(InMemorySeekableByteChannel(bytes, maxReadSize), codecs)

  private def readFooter(bytes: Array[Byte], maxReadSize: Int = Int.MaxValue): SegmentFooter[Int] =
    SegmentReader.readFooter(InMemorySeekableByteChannel(bytes, maxReadSize), codecs)

  private def readEntries(segment: Reader): List[Int] =
    Iterator.continually(segment).takeWhile(_.hasEntry).map(_.readEntry()).toList

  /** Opens the file and reads every entry, as a caller that wants the whole segment does. */
  private def readAll(bytes: Array[Byte]): List[Int] = readEntries(open(bytes))

  private def crc(bytes: Array[Byte]): Int =
    val value = new CRC32C
    value.update(bytes)
    value.getValue.toInt

  private def ints(values: Int*): Array[Byte] =
    val buffer = ByteBuffer.allocate(values.size * 4)
    values.foreach(buffer.putInt)
    buffer.array()

  // [magic:4][payload length:4][header CRC:4][payload:N][payload CRC:4]
  private def frame(magic: Int, payload: Array[Byte]): Array[Byte] =
    val frameHeader = ints(magic, payload.length)
    frameHeader ++ ints(crc(frameHeader)) ++ payload ++ ints(crc(payload))

  private def flipped(bytes: Array[Byte], offset: Int): Array[Byte] =
    val damaged = bytes.clone()
    damaged(offset) = (damaged(offset) ^ 1).toByte
    damaged

  private def headerFrame(format: Int = 1, codecVersion: Int = 1, segmentId: Long = 7L): Array[Byte] =
    frame(
      headerMagic,
      ByteBuffer.allocate(28).putInt(format).putLong(segmentId).putInt(codecVersion).putLong(100L).putInt(5).array()
    )

  private def entryFrame(value: Int): Array[Byte] = frame(entryMagic, ints(value))

  private def footerFrame(count: Long = 3L, created: Long = 100L, codecVersion: Int = 1): Array[Byte] =
    frame(
      footerMagic,
      ByteBuffer.allocate(32).putLong(count).putInt(codecVersion).putLong(created).putLong(200L).putInt(60).array()
    )

  private def trailer(footer: Array[Byte]): Array[Byte] = ints(footer.length, endMagic)

  /** `frames` under the footer and its trailer. */
  private def seal(frames: Array[Byte]): Array[Byte] = frames ++ footer ++ trailer(footer)

  private val header          = headerFrame()
  private val entries         = List(10, 20, 30).map(entryFrame)
  private val footer          = footerFrame()
  private val unsealedSegment = (header :: entries).reduce(_ ++ _)
  private val sealedSegment   = seal(unsealedSegment)
  private val entryStarts     = entries.scanLeft(header.length)(_ + _.length)
  private val footerStart     = unsealedSegment.length

  test("reads the header, the footer and the entries of a sealed segment"):
    val segment = open(sealedSegment)
    segment.header shouldBe SegmentHeader(1, 7L, 1, 100L, 5)
    segment.footer shouldBe SegmentFooter(3L, 1, 100L, 200L, 60)
    readEntries(segment) shouldBe List(10, 20, 30)
    segment.hasEntry shouldBe false

  test("partial channel reads return the same segment"):
    val segment = open(sealedSegment, maxReadSize = 1)
    segment.header shouldBe open(sealedSegment).header
    segment.footer shouldBe open(sealedSegment).footer
    readEntries(segment) shouldBe List(10, 20, 30)

  test("the header is read alone, whatever follows it"):
    val channel = InMemorySeekableByteChannel(header ++ new Array[Byte](20))
    SegmentReader.readHeader(channel, codecs) shouldBe SegmentHeader(1, 7L, 1, 100L, 5)

  test("the footer is read alone, whatever comes before it"):
    val garbage = new Array[Byte](20) ++ footer ++ trailer(footer)
    for bytes <- List(sealedSegment, garbage); maxReadSize <- List(1, Int.MaxValue) do
      readFooter(bytes, maxReadSize) shouldBe SegmentFooter(3L, 1, 100L, 200L, 60)

  test("reading past the last entry throws NoSuchElementException"):
    val segment = open(sealedSegment)
    readEntries(segment)
    intercept[NoSuchElementException](segment.readEntry())

  test("every truncation of a sealed segment is corruption"):
    for cut <- 0 until sealedSegment.length do
      withClue(s"cut at $cut: ")(intercept[CorruptLogException](readAll(sealedSegment.take(cut))))

  test("a failed checksum is an InvalidChecksumException"):
    val offsets = Map(
      "the header's frame header" -> 0,
      "the header's payload"      -> 12,
      "an entry's frame header"   -> entryStarts(1),
      "an entry's payload"        -> (entryStarts(1) + 12),
      "the footer's frame header" -> footerStart,
      "the footer's payload"      -> (footerStart + 12)
    )
    for (name, offset) <- offsets do
      withClue(s"$name: ")(intercept[InvalidChecksumException](readAll(flipped(sealedSegment, offset))))

  test("a file without a seal that checks out is corruption"):
    val cases = Map(
      "no footer"                  -> unsealedSegment,
      "no trailer"                 -> (unsealedSegment ++ footer),
      "a wrong end magic"          -> (unsealedSegment ++ footer ++ ints(footer.length, 0)),
      "a wrong footer length"      -> (unsealedSegment ++ footer ++ ints(footer.length + 1, endMagic)),
      "a trailer without a footer" -> (unsealedSegment ++ ints(footer.length, endMagic)),
      "a trailer over an entry"    -> (unsealedSegment ++ ints(entries.last.length, endMagic)),
      "bytes after the trailer"    -> (sealedSegment :+ 0.toByte)
    )
    for (name, bytes) <- cases do
      withClue(s"$name: "):
        intercept[CorruptLogException](open(bytes))
        intercept[CorruptLogException](readFooter(bytes))

  test("a damaged footer fails reading it alone"):
    intercept[InvalidChecksumException](readFooter(flipped(sealedSegment, footerStart)))
    intercept[InvalidChecksumException](readFooter(flipped(sealedSegment, footerStart + 12)))
    val noEntries = footerFrame(count = 0L)
    intercept[CorruptLogException](readFooter(noEntries ++ trailer(noEntries)))

  test("bytes a crash cannot leave throw CorruptLogException"):
    val negativeLength = ints(entryMagic, -1)
    val noEntries      = footerFrame(count = 0L)
    val cases          = Map(
      "a file that starts with an entry"  -> seal(entries.head),
      "a header without its fixed fields" -> seal(frame(headerMagic, ints(1))),
      "a negative segment ID"             -> seal(headerFrame(segmentId = -1L)),
      "an unknown frame magic"            -> seal(header ++ frame(0x12345678, ints(1))),
      "a second header frame"             -> seal(header ++ header),
      "a negative payload length"         -> seal(header ++ negativeLength ++ ints(crc(negativeLength), 0)),
      "an empty entry payload"            -> seal(header ++ frame(entryMagic, Array.emptyByteArray)),
      "bytes after an entry value"        -> seal(header ++ frame(entryMagic, ints(1, 2))),
      "an entry the codec cannot read"    -> seal(header ++ frame(entryMagic, Array[Byte](1, 2))),
      "a footer frame before the footer"  -> seal(unsealedSegment ++ footer),
      "a footer that counts no entries"   -> (unsealedSegment ++ noEntries ++ trailer(noEntries))
    )
    for (name, bytes) <- cases do withClue(s"$name: ")(intercept[CorruptLogException](readAll(bytes)))

  test("checking the footer against the header and the entries is left to the caller"):
    val mismatched = footerFrame(count = 2L, created = 101L)
    val segment    = open(unsealedSegment ++ mismatched ++ trailer(mismatched))
    segment.footer shouldBe SegmentFooter(2L, 1, 101L, 200L, 60)
    readEntries(segment) shouldBe List(10, 20, 30)

  test("an unknown storage format or codec version throws UnsupportedLogVersionException"):
    intercept[UnsupportedLogVersionException](open(seal(headerFrame(format = 2))))
    intercept[UnsupportedLogVersionException](open(seal(headerFrame(codecVersion = 2))))
    // The footer is decoded with the codecs of the version it names.
    val unknown = footerFrame(codecVersion = 2)
    intercept[UnsupportedLogVersionException](open(unsealedSegment ++ unknown ++ trailer(unknown)))
    intercept[UnsupportedLogVersionException](readFooter(unknown ++ trailer(unknown)))
