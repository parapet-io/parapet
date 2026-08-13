package io.parapet.core.journal

import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.charset.StandardCharsets.UTF_8

class JournalSegmentBinaryFormatSpec extends AnyFunSuite:

  import JournalSegmentBinaryFormat.CorruptSegmentException

  private val a = ProcessRef[Event]("a")
  private val b = ProcessRef[Event]("b")

  private def entry(seq: Long, id: Long = 0L, cause: Long = 0L) =
    JournalEntry(seq, id, a, b, cause, s"e$seq".getBytes(UTF_8))

  private def segment(entries: JournalEntry*): JournalSegment =
    val v = entries.toVector
    JournalSegment(JournalMetadata.of(v, createdAt = 1234L), v)

  test("encode/decode round-trips metadata and entries") {
    val original = segment(entry(3, id = 10L, cause = 4L), entry(7, id = 2L, cause = 20L))
    val decoded  = JournalSegmentBinaryFormat.decode(JournalSegmentBinaryFormat.encode(original))

    decoded.metadata shouldBe original.metadata
    decoded.metadata.minSeq shouldBe 3L
    decoded.metadata.maxSeq shouldBe 7L
    decoded.metadata.entryCount shouldBe 2
    decoded.metadata.maxEnvelopeId shouldBe 20L
    decoded.metadata.createdAt shouldBe 1234L

    decoded.entries.map(_.seq) shouldBe Vector(3L, 7L)
    decoded.entries.map(e => new String(e.event, UTF_8)) shouldBe Vector("e3", "e7")
    decoded.entries.map(_.receiver) shouldBe Vector(b, b)
  }

  test("readMetadata decodes the header from only the leading bytes") {
    val original = segment(entry(5), entry(9))
    val bytes    = JournalSegmentBinaryFormat.encode(original)
    val header   = java.util.Arrays.copyOfRange(bytes, 0, JournalSegmentBinaryFormat.MetadataBytes)

    JournalSegmentBinaryFormat.readMetadata(header) shouldBe original.metadata
  }

  test("readMetadata rejects a truncated header") {
    val bytes = JournalSegmentBinaryFormat.encode(segment(entry(1)))
    val short = java.util.Arrays.copyOfRange(bytes, 0, JournalSegmentBinaryFormat.MetadataBytes - 1)
    an[CorruptSegmentException] should be thrownBy JournalSegmentBinaryFormat.readMetadata(short)
  }

  test("readMetadata rejects bad magic") {
    val bytes = JournalSegmentBinaryFormat.encode(segment(entry(1)))
    bytes(0) = (bytes(0) ^ 0xff).toByte
    an[CorruptSegmentException] should be thrownBy JournalSegmentBinaryFormat.readMetadata(bytes)
  }

  test("readMetadata rejects an unsupported version") {
    val bytes = JournalSegmentBinaryFormat.encode(segment(entry(1)))
    bytes(5) = (bytes(5) + 1).toByte // version Short follows the 4-byte magic
    an[CorruptSegmentException] should be thrownBy JournalSegmentBinaryFormat.readMetadata(bytes)
  }

  test("readMetadata rejects a header checksum mismatch") {
    val bytes = JournalSegmentBinaryFormat.encode(segment(entry(1)))
    bytes(6) = (bytes(6) ^ 0xff).toByte // first byte of the fields region
    an[CorruptSegmentException] should be thrownBy JournalSegmentBinaryFormat.readMetadata(bytes)
  }

  test("decode rejects an entry-count mismatch between header and entries") {
    val entries = Vector(entry(1), entry(2))
    // header claims 3 entries but only 2 are encoded
    val lying = JournalSegment(JournalMetadata(1L, 2L, 3, 0L, 0L), entries)
    an[CorruptSegmentException] should be thrownBy JournalSegmentBinaryFormat.decode(
      JournalSegmentBinaryFormat.encode(lying)
    )
  }
