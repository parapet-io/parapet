package io.parapet.core.snapshot

import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.charset.StandardCharsets.UTF_8

class SnapshotBinaryFormatSpec extends AnyFunSuite:

  private val ref = ProcessRef[Event]("format-spec/proc a") // non-ASCII-safe ref exercises UTF-8 encoding

  private def snapshot(state: Array[Byte], id: Long = 1L, parentId: Long = 0L, seq: Long = 0L, createdAt: Long = 0L) =
    Snapshot(Snapshot.Metadata(ref, id, parentId, seq, createdAt, schemaVersion = 1L), state)

  test("encode/decode round-trips metadata and data") {
    val original = snapshot("hello".getBytes(UTF_8), id = 3L, parentId = 2L, seq = 42L, createdAt = 1234L)
    val decoded  = SnapshotBinaryFormat.decode(SnapshotBinaryFormat.encode(original))

    decoded.metadata shouldBe original.metadata
    new String(decoded.data, UTF_8) shouldBe "hello"
  }

  test("round-trips empty state") {
    val decoded = SnapshotBinaryFormat.decode(SnapshotBinaryFormat.encode(snapshot(Array.emptyByteArray)))
    decoded.data shouldBe empty
    decoded.metadata.processRef shouldBe ref
  }

  test("bytes are backend-independent: a blob decodes without any storage instance") {
    val blob = SnapshotBinaryFormat.encode(snapshot("state".getBytes(UTF_8), id = 7L))
    SnapshotBinaryFormat.decode(blob).metadata.id shouldBe 7L
  }

  test("a foreign blob is rejected by magic") {
    val error = the[SnapshotBinaryFormat.CorruptSnapshotException] thrownBy
      SnapshotBinaryFormat.decode("not a snapshot".getBytes(UTF_8))
    error.getMessage should include("magic")
  }

  test("a truncated blob is rejected") {
    val blob = SnapshotBinaryFormat.encode(snapshot("state".getBytes(UTF_8)))
    a[SnapshotBinaryFormat.CorruptSnapshotException] should be thrownBy
      SnapshotBinaryFormat.decode(blob.take(blob.length - 4)) // drop the data checksum
  }

  test("a flipped data byte fails the checksum") {
    val blob = SnapshotBinaryFormat.encode(snapshot("state".getBytes(UTF_8)))
    blob(blob.length - 6) = (blob(blob.length - 6) ^ 0xff).toByte // inside the data payload
    val error = the[SnapshotBinaryFormat.CorruptSnapshotException] thrownBy SnapshotBinaryFormat.decode(blob)
    error.getMessage should include("checksum")
  }
