package io.parapet.snapshot

import io.parapet.snapshot.{Snapshot, SnapshotBinaryFormat}
import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util.zip.CRC32

class SnapshotBinaryFormatSpec extends AnyFunSuite:

  private val ref = ProcessRef[Event]("format-spec/proc a") // non-ASCII-safe ref exercises UTF-8 encoding

  private def snapshot(state: Array[Byte], id: Long = 1L, parentId: Long = 0L, seq: Long = 0L, createdAt: Long = 0L) =
    Snapshot(Snapshot.Metadata(ref, id, parentId, seq, createdAt, schemaVersion = 1L), state)

  private def crc32(bytes: Array[Byte]): Int =
    val crc = new CRC32(); crc.update(bytes); crc.getValue.toInt

  /** Assembles a well-framed blob (valid magic/version and per-section checksums) from raw metadata and data payloads,
    * so a test can feed a *structurally valid* envelope whose metadata bytes are internally malformed.
    */
  private def frame(meta: Array[Byte], data: Array[Byte] = "data".getBytes(UTF_8)): Array[Byte] =
    val buf = ByteBuffer.allocate(4 + 2 + 4 + meta.length + 4 + 4 + data.length + 4)
    buf.putInt(0x50534e50) // magic "PSNP"
    buf.putShort(1)        // format version
    buf.putInt(meta.length); buf.put(meta); buf.putInt(crc32(meta))
    buf.putInt(data.length); buf.put(data); buf.putInt(crc32(data))
    buf.array()

  /** Raw metadata payload: `[refLength][refBytes][trailing]`, with a caller-controlled (possibly bogus) refLength. */
  private def metaBytes(
      refLength: Int,
      refBytes: Array[Byte],
      trailing: Array[Byte] = Array.emptyByteArray
  ): Array[Byte] =
    val buf = ByteBuffer.allocate(4 + refBytes.length + trailing.length)
    buf.putInt(refLength); buf.put(refBytes); buf.put(trailing)
    buf.array()

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

  // The metadata section below is CRC-valid (frame recomputes the checksum), so decode reaches decodeMetadata; only
  // the ref length inside it is bogus. These guard the length bound before allocation - a raw ByteBuffer would throw
  // BufferUnderflowException (or OOM on a huge length), which must surface as CorruptSnapshotException instead.

  test("an out-of-range ref length is rejected as corrupt, not underflow/OOM") {
    val blob = frame(metaBytes(refLength = Int.MaxValue, refBytes = Array.emptyByteArray))
    the[SnapshotBinaryFormat.CorruptSnapshotException] thrownBy
      SnapshotBinaryFormat.decode(blob)
  }

  test("a negative ref length is rejected as corrupt") {
    val blob = frame(metaBytes(refLength = -1, refBytes = Array.emptyByteArray))
    the[SnapshotBinaryFormat.CorruptSnapshotException] thrownBy
      SnapshotBinaryFormat.decode(blob)
  }

  test("an empty ref is rejected as corrupt") {
    val blob  = frame(metaBytes(refLength = 0, refBytes = Array.emptyByteArray, trailing = new Array[Byte](40)))
    val error = the[SnapshotBinaryFormat.CorruptSnapshotException] thrownBy SnapshotBinaryFormat.decode(blob)
    error.getMessage should include("empty process ref")
  }

  test("metadata truncated before its long fields is rejected as corrupt") {
    // valid ref, but only one long's worth of trailing bytes where five are required
    val ref   = "abc".getBytes(UTF_8)
    val blob  = frame(metaBytes(refLength = ref.length, refBytes = ref, trailing = new Array[Byte](8)))
    val error = the[SnapshotBinaryFormat.CorruptSnapshotException] thrownBy SnapshotBinaryFormat.decode(blob)
    error.getMessage should include("truncated metadata fields")
  }
