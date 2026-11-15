package io.parapet.journal

import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.charset.StandardCharsets.UTF_8

class JournalEntryBinaryFormatSpec extends AnyFunSuite:

  private val sender   = ProcessRef[Event]("journal-spec/sender a") // fs/UTF-8-exercising refs
  private val receiver = ProcessRef[Event]("journal-spec/receiver b")

  private def entry(
      seq: Long,
      id: Long = 0L,
      cause: Long = 0L,
      event: String = "event",
      kind: JournalEntry.Kind = JournalEntry.Kind.Event
  ) =
    JournalEntry(seq, id, sender, receiver, cause, event.getBytes(UTF_8), kind)

  private def sameAs(a: JournalEntry, b: JournalEntry): Unit =
    a.seq shouldBe b.seq
    a.id shouldBe b.id
    a.sender shouldBe b.sender
    a.receiver shouldBe b.receiver
    a.cause shouldBe b.cause
    a.event.toVector shouldBe b.event.toVector
    a.kind shouldBe b.kind

  test("encode/decode round-trips one entry") {
    val original = entry(seq = 42L, id = 99L, cause = 7L, event = "hello")
    sameAs(JournalEntryBinaryFormat.decode(JournalEntryBinaryFormat.encode(original)), original)
  }

  test("round-trips runtime registration records independently of payload bytes") {
    val original = entry(1L, kind = JournalEntry.Kind.Registration)
    sameAs(JournalEntryBinaryFormat.decode(JournalEntryBinaryFormat.encode(original)), original)
  }

  test("round-trips an empty event payload") {
    val original = entry(1L, event = "")
    sameAs(JournalEntryBinaryFormat.decode(JournalEntryBinaryFormat.encode(original)), original)
  }

  test("rejects schema-bearing legacy formats instead of silently discarding their metadata") {
    for version <- List(1, 2) do
      val bytes = JournalEntryBinaryFormat.encode(entry(1L))
      java.nio.ByteBuffer.wrap(bytes).putShort(4, version.toShort)
      val error = intercept[JournalEntryBinaryFormat.CorruptEntryException](JournalEntryBinaryFormat.decode(bytes))
      error.getMessage should include("unsupported format version")
  }

  test("encodeBatch/decodeBatch round-trips every entry in order") {
    val entries = Vector(entry(1L), entry(2L, cause = 1L), entry(3L, cause = 2L, event = "third"))
    val decoded = JournalEntryBinaryFormat.decodeBatch(JournalEntryBinaryFormat.encodeBatch(entries))
    decoded.map(_.seq) shouldBe Vector(1L, 2L, 3L)
    decoded.zip(entries).foreach((d, o) => sameAs(d, o))
  }

  test("decodeBatch of an empty batch is empty") {
    JournalEntryBinaryFormat.decodeBatch(JournalEntryBinaryFormat.encodeBatch(Vector.empty)) shouldBe empty
  }

  test("a foreign blob is rejected by magic") {
    val error = the[JournalEntryBinaryFormat.CorruptEntryException] thrownBy
      JournalEntryBinaryFormat.decode("not a journal entry".getBytes(UTF_8))
    error.getMessage should include("magic")
  }

  test("a truncated blob is rejected") {
    val blob = JournalEntryBinaryFormat.encode(entry(1L))
    a[JournalEntryBinaryFormat.CorruptEntryException] should be thrownBy
      JournalEntryBinaryFormat.decode(blob.take(blob.length - 4)) // drop the checksum
  }

  test("a flipped payload byte fails the checksum") {
    val blob = JournalEntryBinaryFormat.encode(entry(1L, event = "state"))
    blob(blob.length - 6) = (blob(blob.length - 6) ^ 0xff).toByte // inside the payload
    val error = the[JournalEntryBinaryFormat.CorruptEntryException] thrownBy JournalEntryBinaryFormat.decode(blob)
    error.getMessage should include("checksum")
  }

  test("decode rejects trailing bytes after one entry") {
    val blob = JournalEntryBinaryFormat.encode(entry(1L))
    a[JournalEntryBinaryFormat.CorruptEntryException] should be thrownBy
      JournalEntryBinaryFormat.decode(blob :+ 0.toByte)
  }
