package io.parapet.journal

import io.parapet.TestUtils.{TestIO, given}
import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, StandardOpenOption}
import scala.jdk.CollectionConverters.*

class JournalStoreLocalSpec extends AnyFunSuite:

  private val a = ProcessRef[Event]("a")
  private val b = ProcessRef[Event]("b")

  private def entry(seq: Long, receiver: ProcessRef.Unknown = a, id: Long = 0L, cause: Long = 0L) =
    JournalEntry(seq, id, a, receiver, cause, s"e$seq".getBytes(UTF_8), tag = "e", schemaVersion = 1)

  private def newStore(
      dir: Path = Files.createTempDirectory("journal-spec"),
      maxSegmentBytes: Long = JournalStoreLocal.DefaultMaxSegmentBytes,
      maxEntryBytes: Int = JournalStoreLocal.DefaultMaxEntryBytes
  ) =
    (
      dir,
      new JournalStoreLocal[TestIO](JournalStoreLocal.Config(dir, maxSegmentBytes, maxEntryBytes))
    )

  private def seqs(entries: Vector[JournalEntry]): Vector[Long] = entries.map(_.seq)

  private def names(dir: Path): Vector[String] =
    val listing = Files.list(dir)
    try listing.iterator().asScala.map(_.getFileName.toString).toVector.sorted
    finally listing.close()

  private def fileWithSuffix(dir: Path, suffix: String): Path =
    val listing = Files.list(dir)
    try
      listing
        .iterator()
        .asScala
        .find(_.getFileName.toString.endsWith(suffix))
        .getOrElse(fail(s"missing $suffix journal file"))
    finally listing.close()

  private def openFile(dir: Path): Path = fileWithSuffix(dir, ".open")

  private def sealedFile(dir: Path): Path = fileWithSuffix(dir, ".jrnl")

  private def highWaterFile(dir: Path): Path = dir.resolve(DeliveryLog.HighWaterFileName)

  private def highWaterTempFile(dir: Path): Path = dir.resolve(DeliveryLog.HighWaterTempFileName)

  test("append then read returns entries after the given seq") {
    val (_, store) = newStore()
    store.append(Vector(entry(1), entry(2), entry(3))).unsafeRun()
    seqs(store.read(0L).unsafeRun()) shouldBe Vector(1, 2, 3)
    seqs(store.read(1L).unsafeRun()) shouldBe Vector(2, 3)
    store.read(3L).unsafeRun() shouldBe empty
  }

  test("maxSegmentBytes reserves space beyond the fixed header and footer") {
    val fixedBytes = DeliveryLog.Header.EncodedSize.toLong + DeliveryLog.Footer.EncodedSize.toLong

    an[IllegalArgumentException] should be thrownBy newStore(maxSegmentBytes = fixedBytes)
    noException should be thrownBy newStore(maxSegmentBytes = fixedBytes + 1L)
  }

  test("successive batches remain ordered and may contain sequence gaps") {
    val (_, store) = newStore()
    store.append(Vector(entry(1), entry(3))).unsafeRun()
    store.append(Vector(entry(5), entry(8))).unsafeRun()
    seqs(store.read(0L).unsafeRun()) shouldBe Vector(1, 3, 5, 8)
  }

  test("maxSeq reflects the highest stored seq") {
    val (_, store) = newStore()
    store.maxSeq.unsafeRun() shouldBe None
    store.append(Vector(entry(2), entry(9))).unsafeRun()
    store.maxSeq.unsafeRun() shouldBe Some(9L)
  }

  test("maxEnvelopeId reflects the highest id or cause stored") {
    val (_, store) = newStore()
    store.maxEnvelopeId.unsafeRun() shouldBe None
    store.append(Vector(entry(1, id = 3L, cause = 1L), entry(2, id = 5L, cause = 9L))).unsafeRun()
    store.append(Vector(entry(3, id = 7L, cause = 2L))).unsafeRun()
    store.maxEnvelopeId.unsafeRun() shouldBe Some(9L)
  }

  test("maxEnvelopeId is recovered from a sealed delivery footer") {
    val rotateAfterEveryBatch = DeliveryLog.Header.EncodedSize + DeliveryLog.Footer.EncodedSize + 1L
    val (dir, store)          = newStore(maxSegmentBytes = rotateAfterEveryBatch)
    store.append(Vector(entry(1, id = 3L, cause = 9L))).unsafeRun()

    val (_, recovered) = newStore(dir, maxSegmentBytes = rotateAfterEveryBatch)
    recovered.maxEnvelopeId.unsafeRun() shouldBe Some(9L)
  }

  test("lowering maxEntryBytes does not make durable entries unreadable") {
    val (dir, writer) = newStore(maxEntryBytes = 1024)
    writer.append(Vector(entry(1))).unsafeRun()

    val (_, reader) = newStore(dir, maxEntryBytes = 1)
    seqs(reader.read(0L).unsafeRun()) shouldBe Vector(1L)
  }

  test("append does not create a delivery high-water checkpoint") {
    val (dir, store) = newStore()
    store.append(Vector(entry(1, id = 3L))).unsafeRun()

    Files.exists(highWaterFile(dir)) shouldBe false
  }

  test("truncate preserves delivery and envelope high-water after removing every segment") {
    val (dir, store) = newStore()
    store.append(Vector(entry(1, id = 3L, cause = 9L))).unsafeRun()
    store.truncate(1L).unsafeRun()

    store.read(0L).unsafeRun() shouldBe empty
    store.maxSeq.unsafeRun() shouldBe Some(1L)
    store.maxEnvelopeId.unsafeRun() shouldBe Some(9L)
    Files.exists(highWaterFile(dir)) shouldBe true

    val (_, recovered) = newStore(dir)
    recovered.read(0L).unsafeRun() shouldBe empty
    recovered.maxSeq.unsafeRun() shouldBe Some(1L)
    recovered.maxEnvelopeId.unsafeRun() shouldBe Some(9L)
  }

  test("append after truncation combines the checkpoint with newer segments") {
    val rotateAfterEveryBatch = DeliveryLog.Header.EncodedSize + DeliveryLog.Footer.EncodedSize + 1L
    val (dir, store)          = newStore(maxSegmentBytes = rotateAfterEveryBatch)
    store.append(Vector(entry(1, id = 3L, cause = 9L))).unsafeRun()
    store.truncate(1L).unsafeRun()
    store.append(Vector(entry(2, id = 11L))).unsafeRun()

    val (_, recovered) = newStore(dir, maxSegmentBytes = rotateAfterEveryBatch)
    seqs(recovered.read(0L).unsafeRun()) shouldBe Vector(2L)
    recovered.maxSeq.unsafeRun() shouldBe Some(2L)
    recovered.maxEnvelopeId.unsafeRun() shouldBe Some(11L)
  }

  test("a sequence below the checkpoint cannot be reused after restart") {
    val rotateAfterEveryBatch = DeliveryLog.Header.EncodedSize + DeliveryLog.Footer.EncodedSize + 1L
    val (dir, store)          = newStore(maxSegmentBytes = rotateAfterEveryBatch)
    store.append(Vector(entry(1))).unsafeRun()
    store.truncate(1L).unsafeRun()

    val (_, recovered) = newStore(dir, maxSegmentBytes = rotateAfterEveryBatch)
    an[IllegalStateException] should be thrownBy recovered.append(Vector(entry(1))).unsafeRun()
  }

  test("successive truncations advance the same delivery high-water checkpoint") {
    val rotateAfterEveryBatch = DeliveryLog.Header.EncodedSize + DeliveryLog.Footer.EncodedSize + 1L
    val (dir, store)          = newStore(maxSegmentBytes = rotateAfterEveryBatch)
    store.append(Vector(entry(1, id = 3L))).unsafeRun()
    store.truncate(1L).unsafeRun()
    store.append(Vector(entry(2, id = 7L))).unsafeRun()
    store.truncate(2L).unsafeRun()

    names(dir).count(_ == DeliveryLog.HighWaterFileName) shouldBe 1
    val (_, recovered) = newStore(dir, maxSegmentBytes = rotateAfterEveryBatch)
    recovered.maxSeq.unsafeRun() shouldBe Some(2L)
    recovered.maxEnvelopeId.unsafeRun() shouldBe Some(7L)
  }

  test("a stale high-water temporary file does not supersede the checkpoint") {
    val rotateAfterEveryBatch = DeliveryLog.Header.EncodedSize + DeliveryLog.Footer.EncodedSize + 1L
    val (dir, store)          = newStore(maxSegmentBytes = rotateAfterEveryBatch)
    store.append(Vector(entry(1, id = 7L))).unsafeRun()
    store.truncate(1L).unsafeRun()
    Files.write(highWaterTempFile(dir), Array[Byte](1, 2, 3))

    val (_, recovered) = newStore(dir, maxSegmentBytes = rotateAfterEveryBatch)
    recovered.maxSeq.unsafeRun() shouldBe Some(1L)
    recovered.maxEnvelopeId.unsafeRun() shouldBe Some(7L)
  }

  test("a corrupt delivery high-water checkpoint fails loud") {
    val rotateAfterEveryBatch = DeliveryLog.Header.EncodedSize + DeliveryLog.Footer.EncodedSize + 1L
    val (dir, store)          = newStore(maxSegmentBytes = rotateAfterEveryBatch)
    store.append(Vector(entry(1))).unsafeRun()
    store.truncate(1L).unsafeRun()
    val path  = highWaterFile(dir)
    val bytes = Files.readAllBytes(path)
    bytes(bytes.length - 1) = (bytes.last ^ 0xff).toByte
    Files.write(path, bytes, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)

    val (_, recovered) = newStore(dir, maxSegmentBytes = rotateAfterEveryBatch)
    val error          = the[DeliveryLog.CorruptLogException] thrownBy recovered.maxSeq.unsafeRun()
    error.getMessage should include("high-water checksum")
  }

  test("truncate drops files fully at or below upToSeq and keeps the rest") {
    val rotateAfterEveryBatch = DeliveryLog.Header.EncodedSize + DeliveryLog.Footer.EncodedSize + 1L
    val (_, store)            = newStore(maxSegmentBytes = rotateAfterEveryBatch)
    store.append(Vector(entry(1), entry(2))).unsafeRun()
    store.append(Vector(entry(3), entry(4))).unsafeRun()
    store.append(Vector(entry(5), entry(6))).unsafeRun()
    store.truncate(4L).unsafeRun()
    seqs(store.read(0L).unsafeRun()) shouldBe Vector(5, 6)
  }

  test("truncate keeps a file straddling upToSeq whole") {
    val rotateAfterEveryBatch = DeliveryLog.Header.EncodedSize + DeliveryLog.Footer.EncodedSize + 1L
    val (dir, store)          = newStore(maxSegmentBytes = rotateAfterEveryBatch)
    store.append(Vector(entry(3), entry(7))).unsafeRun()
    store.truncate(5L).unsafeRun()
    seqs(store.read(0L).unsafeRun()) shouldBe Vector(3, 7)
    Files.exists(highWaterFile(dir)) shouldBe false
  }

  test("an empty append writes nothing") {
    val (dir, store) = newStore()
    store.append(Vector.empty).unsafeRun()
    store.read(0L).unsafeRun() shouldBe empty
    store.maxSeq.unsafeRun() shouldBe None
    Files.exists(dir) shouldBe true
    names(dir) shouldBe empty
  }

  test("a new store recovers the active file") {
    val (dir, first) = newStore()
    first.append(Vector(entry(1), entry(2))).unsafeRun()
    names(dir).count(_.endsWith(".open")) shouldBe 1

    val (_, second) = newStore(dir)
    seqs(second.read(0L).unsafeRun()) shouldBe Vector(1, 2)
    second.maxSeq.unsafeRun() shouldBe Some(2L)
    names(dir).count(_.endsWith(".open")) shouldBe 0
    names(dir).count(_.endsWith(".jrnl")) shouldBe 1
  }

  test("read preserves receiver and payload") {
    val (_, store) = newStore()
    store.append(Vector(entry(1, receiver = b))).unsafeRun()
    val loaded = store.read(0L).unsafeRun().head
    loaded.receiver shouldBe b
    new String(loaded.event, UTF_8) shouldBe "e1"
  }

  test("read of a non-existent data dir is empty") {
    val absent     = Files.createTempDirectory("journal-cold").resolve("nope")
    val (_, store) = newStore(absent)
    store.read(0L).unsafeRun() shouldBe empty
    store.maxSeq.unsafeRun() shouldBe None
  }

  test("append creates an absent data directory") {
    val dir        = Files.createTempDirectory("journal-parent").resolve("nested").resolve("journal")
    val (_, store) = newStore(dir)

    store.append(Vector(entry(1))).unsafeRun()

    Files.isDirectory(dir) shouldBe true
    seqs(store.read(0L).unsafeRun()) shouldBe Vector(1L)
  }

  test("recovery removes an active file without a complete header") {
    val dir  = Files.createTempDirectory("journal-empty-active")
    val path = dir.resolve("00000000000000000001.open")
    Files.write(path, Array.emptyByteArray)
    val (_, store) = newStore(dir)

    store.read(0L).unsafeRun() shouldBe empty

    Files.exists(path) shouldBe false
  }

  test("read fails fast when the data dir path is not a directory") {
    val file       = Files.createTempFile("journal-not-a-dir", ".tmp")
    val (_, store) = newStore(file)
    an[java.io.IOException] should be thrownBy store.read(0L).unsafeRun()
  }

  test("a malformed sealed filename fails loud") {
    val rotateAfterEveryBatch = DeliveryLog.Header.EncodedSize + DeliveryLog.Footer.EncodedSize + 1L
    val (dir, store)          = newStore(maxSegmentBytes = rotateAfterEveryBatch)
    store.append(Vector(entry(1))).unsafeRun()
    Files.move(sealedFile(dir), dir.resolve("garbage.jrnl"))

    val (_, recovered) = newStore(dir, maxSegmentBytes = rotateAfterEveryBatch)
    val error          = the[DeliveryLog.CorruptLogException] thrownBy recovered.maxSeq.unsafeRun()
    error.getMessage should include("malformed sealed filename")
  }

  test("a sealed filename range must match its metadata") {
    val rotateAfterEveryBatch = DeliveryLog.Header.EncodedSize + DeliveryLog.Footer.EncodedSize + 1L
    val (dir, store)          = newStore(maxSegmentBytes = rotateAfterEveryBatch)
    store.append(Vector(entry(1))).unsafeRun()
    Files.move(
      sealedFile(dir),
      dir.resolve("00000000000000000002-00000000000000000002.jrnl")
    )

    val (_, recovered) = newStore(dir, maxSegmentBytes = rotateAfterEveryBatch)
    val error          = the[DeliveryLog.CorruptLogException] thrownBy recovered.maxSeq.unsafeRun()
    error.getMessage should include("does not match metadata")
  }

  test("multiple active delivery files fail loud") {
    val (dir, store) = newStore()
    Files.write(dir.resolve("00000000000000000001.open"), Array.emptyByteArray)
    Files.write(dir.resolve("00000000000000000002.open"), Array.emptyByteArray)

    an[DeliveryLog.CorruptLogException] should be thrownBy store.maxSeq.unsafeRun()
  }

  test("recorder batches append to one active file until rotation") {
    val (dir, store) = newStore()
    store.append(Vector(entry(1), entry(2))).unsafeRun()
    store.append(Vector(entry(3), entry(4))).unsafeRun()

    names(dir).count(_.endsWith(".open")) shouldBe 1
    names(dir).count(_.endsWith(".jrnl")) shouldBe 0
    seqs(store.read(0L).unsafeRun()) shouldBe Vector(1, 2, 3, 4)
  }

  test("rotation seals the active file by delivery sequence range") {
    val rotateAfterEveryBatch = DeliveryLog.Header.EncodedSize + DeliveryLog.Footer.EncodedSize + 1L
    val (dir, store)          = newStore(maxSegmentBytes = rotateAfterEveryBatch)
    store.append(Vector(entry(1), entry(2))).unsafeRun()
    store.append(Vector(entry(4))).unsafeRun()

    names(dir) should contain("00000000000000000001-00000000000000000002.jrnl")
    names(dir) should contain("00000000000000000004-00000000000000000004.jrnl")
  }

  test("recovery truncates an incomplete final frame and preserves its valid prefix") {
    val (dir, store) = newStore()
    store.append(Vector(entry(1), entry(2))).unsafeRun()
    Files.write(openFile(dir), Array[Byte](0x50, 0x41), StandardOpenOption.WRITE, StandardOpenOption.APPEND)

    val (_, recovered) = newStore(dir)
    seqs(recovered.read(0L).unsafeRun()) shouldBe Vector(1, 2)
  }

  test("recovery replaces an interrupted footer") {
    val (dir, store) = newStore()
    store.append(Vector(entry(1), entry(2))).unsafeRun()
    val footerPrefix = ByteBuffer
      .allocate(DeliveryLog.MagicSize + java.lang.Long.BYTES)
      .putInt(DeliveryLog.Footer.StartMagic)
      .putLong(1L)
      .array()
    Files.write(openFile(dir), footerPrefix, StandardOpenOption.WRITE, StandardOpenOption.APPEND)

    val (_, recovered) = newStore(dir)
    seqs(recovered.read(0L).unsafeRun()) shouldBe Vector(1, 2)
    names(dir).count(_.endsWith(".open")) shouldBe 0
    names(dir).count(_.endsWith(".jrnl")) shouldBe 1
  }

  test("recovery replaces a complete footer left in an active file") {
    val rotateAfterEveryBatch = DeliveryLog.Header.EncodedSize + DeliveryLog.Footer.EncodedSize + 1L
    val (dir, store)          = newStore(maxSegmentBytes = rotateAfterEveryBatch)
    store.append(Vector(entry(1), entry(2))).unsafeRun()
    Files.move(sealedFile(dir), dir.resolve("00000000000000000001.open"))

    val (_, recovered) = newStore(dir, maxSegmentBytes = rotateAfterEveryBatch)
    seqs(recovered.read(0L).unsafeRun()) shouldBe Vector(1, 2)
    names(dir).count(_.endsWith(".open")) shouldBe 0
    names(dir).count(_.endsWith(".jrnl")) shouldBe 1
  }

  test("recovery rejects an invalid final frame magic") {
    val (dir, store) = newStore()
    store.append(Vector(entry(1))).unsafeRun()
    Files.write(
      openFile(dir),
      Array[Byte](0x01, 0x02, 0x03, 0x04),
      StandardOpenOption.WRITE,
      StandardOpenOption.APPEND
    )

    val (_, recovered) = newStore(dir)
    val error          = the[DeliveryLog.CorruptLogException] thrownBy recovered.read(0L).unsafeRun()
    error.getMessage should include("bad entry magic")
  }

  test("recovery rejects a complete final frame with a bad checksum") {
    val (dir, store) = newStore()
    store.append(Vector(entry(1))).unsafeRun()
    val path  = openFile(dir)
    val bytes = Files.readAllBytes(path)
    bytes(bytes.length - 1) = (bytes.last ^ 0xff).toByte
    Files.write(path, bytes, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)

    val (_, recovered) = newStore(dir)
    val error          = the[DeliveryLog.CorruptLogException] thrownBy recovered.read(0L).unsafeRun()
    error.getMessage should include("checksum")
  }

  test("reusing a durable sequence fails even when the entry is byte-identical") {
    val (_, store) = newStore()
    val entries    = Vector(entry(1), entry(2))
    store.append(entries).unsafeRun()
    an[IllegalStateException] should be thrownBy store.append(entries).unsafeRun()
  }

  test("reusing a sequence for different data fails") {
    val (_, store) = newStore()
    store.append(Vector(entry(1))).unsafeRun()
    an[IllegalStateException] should be thrownBy
      store.append(Vector(entry(1, id = 99L))).unsafeRun()
  }

  test("a missing sequence below the durable high-water cannot be filled later") {
    val (_, store) = newStore()
    store.append(Vector(entry(1), entry(3))).unsafeRun()
    an[IllegalStateException] should be thrownBy store.append(Vector(entry(2))).unsafeRun()
  }
