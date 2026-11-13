package io.parapet.journal

import io.parapet.TestUtils.{TestIO, given}
import io.parapet.journal.log.InvalidChecksumException
import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, StandardOpenOption}
import scala.jdk.CollectionConverters.*

class JournalStoreLocalSpec extends AnyFunSuite:

  private val a = ProcessRef[Event]("a")
  private val b = ProcessRef[Event]("b")

  /** With this limit every entry seals the one before it, so only the last entry is in the segment being written. */
  private val sealEveryEntry = 1L

  private def entry(seq: Long, receiver: ProcessRef.Unknown = a, id: Long = 0L, cause: Long = 0L) =
    JournalEntry(
      seq,
      id,
      a,
      receiver,
      cause,
      s"e$seq".getBytes(UTF_8),
      schema = io.parapet.EventCodec.Schema.Simple("test", "e"),
      schemaVersion = 1
    )

  private def newDirectory(): Path = Files.createTempDirectory("journal-spec")

  /** A store for `dir` that is not open yet. */
  private def unopened(
      dir: Path,
      maxSegmentBytes: Long = JournalStoreLocal.DefaultMaxSegmentBytes,
      maxEntryBytes: Int = JournalStoreLocal.DefaultMaxEntryBytes,
      durability: JournalDurability = JournalDurability.HostCrash
  ): JournalStoreLocal[TestIO] =
    new JournalStoreLocal[TestIO](JournalStoreLocal.Config(dir, maxSegmentBytes, maxEntryBytes, durability))

  /** An open store for `dir`. */
  private def store(
      dir: Path,
      maxSegmentBytes: Long = JournalStoreLocal.DefaultMaxSegmentBytes,
      maxEntryBytes: Int = JournalStoreLocal.DefaultMaxEntryBytes,
      durability: JournalDurability = JournalDurability.HostCrash
  ): JournalStoreLocal[TestIO] =
    val journal = unopened(dir, maxSegmentBytes, maxEntryBytes, durability)
    journal.open().unsafeRun()
    journal

  private def seqs(entries: Vector[JournalEntry]): Vector[Long] = entries.map(_.seq)

  private def names(dir: Path): Vector[String] =
    val listing = Files.list(dir)
    try listing.iterator().asScala.map(_.getFileName.toString).toVector.sorted
    finally listing.close()

  test("host-crash durability is the default") {
    JournalConfig.default.durability shouldBe JournalDurability.HostCrash
    JournalStoreLocal.Config(newDirectory()).durability shouldBe JournalDurability.HostCrash
  }

  test("the segment and entry limits must be positive") {
    an[IllegalArgumentException] should be thrownBy unopened(newDirectory(), maxSegmentBytes = 0L)
    an[IllegalArgumentException] should be thrownBy unopened(newDirectory(), maxEntryBytes = 0)
  }

  test("open creates the data directory and the log's files, and close records a clean stop") {
    val dir     = newDirectory().resolve("nested").resolve("journal")
    val journal = unopened(dir)
    Files.exists(dir) shouldBe false
    journal.open().unsafeRun()
    names(dir) shouldBe Vector("open", "running")
    journal.read(0L).unsafeRun() shouldBe empty
    journal.maxSeq.unsafeRun() shouldBe None
    journal.maxEnvelopeId.unsafeRun() shouldBe None
    journal.append(Vector(entry(1))).unsafeRun()
    journal.close().unsafeRun()
    names(dir) shouldBe Vector("open")
  }

  /** Every method that needs an open store. */
  private def calls(journal: JournalStoreLocal[TestIO]): Map[String, TestIO[Any]] = Map(
    "append"        -> journal.append(Vector(entry(1))),
    "empty append"  -> journal.append(Vector.empty),
    "read"          -> journal.read(0L),
    "maxSeq"        -> journal.maxSeq,
    "maxEnvelopeId" -> journal.maxEnvelopeId,
    "truncate"      -> journal.truncate(1L)
  )

  test("a store that is not open rejects every call but open and close, and touches no file") {
    val dir     = newDirectory()
    val journal = unopened(dir)
    for (name, call) <- calls(journal) do
      withClue(s"$name: ")(an[IllegalArgumentException] should be thrownBy call.unsafeRun())
    journal.close().unsafeRun()
    names(dir) shouldBe empty
  }

  test("a closed store rejects every call") {
    val journal = store(newDirectory())
    journal.append(Vector(entry(1))).unsafeRun()
    journal.close().unsafeRun()
    for (name, call) <- calls(journal) do
      withClue(s"$name: ")(an[IllegalArgumentException] should be thrownBy call.unsafeRun())
    // Closing again does nothing.
    journal.close().unsafeRun()
  }

  test("a store is opened only once") {
    val journal = store(newDirectory())
    an[IllegalArgumentException] should be thrownBy journal.open().unsafeRun()
    journal.append(Vector(entry(1))).unsafeRun()
    journal.close().unsafeRun()
    an[IllegalArgumentException] should be thrownBy journal.open().unsafeRun()
  }

  test("an empty append writes nothing") {
    val dir     = newDirectory()
    val journal = store(dir)
    journal.append(Vector.empty).unsafeRun()
    journal.read(0L).unsafeRun() shouldBe empty
    journal.maxSeq.unsafeRun() shouldBe None
    Files.size(dir.resolve("open")) shouldBe 0L
  }

  test("a data dir path that is not a directory fails the open") {
    val journal = unopened(Files.createTempFile("journal-not-a-dir", ".tmp"))
    an[java.io.IOException] should be thrownBy journal.open().unsafeRun()
  }

  test("append then read returns the entries after the given seq") {
    val journal = store(newDirectory())
    journal.append(Vector(entry(1), entry(2), entry(3))).unsafeRun()
    seqs(journal.read(0L).unsafeRun()) shouldBe Vector(1, 2, 3)
    seqs(journal.read(1L).unsafeRun()) shouldBe Vector(2, 3)
    journal.read(3L).unsafeRun() shouldBe empty
  }

  test("read returns the entries of sealed segments and of the segment being written, in order") {
    val dir     = newDirectory()
    val journal = store(dir, maxSegmentBytes = sealEveryEntry)
    journal.append(Vector(entry(1), entry(2))).unsafeRun()
    journal.append(Vector(entry(4))).unsafeRun()
    names(dir) shouldBe Vector("1-1.jrl", "2-2.jrl", "open", "running")
    seqs(journal.read(0L).unsafeRun()) shouldBe Vector(1, 2, 4)
    seqs(journal.read(2L).unsafeRun()) shouldBe Vector(4)
  }

  test("read preserves every field of an entry") {
    val journal = store(newDirectory())
    journal.append(Vector(entry(1, receiver = b, id = 4L, cause = 2L))).unsafeRun()
    val loaded = journal.read(0L).unsafeRun().head
    loaded.seq shouldBe 1L
    loaded.id shouldBe 4L
    loaded.sender shouldBe a
    loaded.receiver shouldBe b
    loaded.cause shouldBe 2L
    new String(loaded.event, UTF_8) shouldBe "e1"
    loaded.schema shouldBe io.parapet.EventCodec.Schema.Simple("test", "e")
    loaded.schemaVersion shouldBe 1
  }

  test("batches go to one segment until it is full") {
    val dir     = newDirectory()
    val journal = store(dir)
    journal.append(Vector(entry(1), entry(2))).unsafeRun()
    journal.append(Vector(entry(3), entry(4))).unsafeRun()
    names(dir) shouldBe Vector("open", "running")
    seqs(journal.read(0L).unsafeRun()) shouldBe Vector(1, 2, 3, 4)
  }

  test("entries survive a clean restart under both durabilities, sealed under their sequence range") {
    for durability <- List(JournalDurability.HostCrash, JournalDurability.ProcessCrash) do
      val dir   = newDirectory()
      val first = store(dir, durability = durability)
      first.append(Vector(entry(1), entry(2))).unsafeRun()
      first.append(Vector(entry(5))).unsafeRun()
      first.close().unsafeRun()

      val second = store(dir, durability = durability)
      seqs(second.read(0L).unsafeRun()) shouldBe Vector(1, 2, 5)
      second.maxSeq.unsafeRun() shouldBe Some(5L)
      names(dir) should contain("1-5.jrl")
      second.close().unsafeRun()
  }

  test("a store that was never closed leaves its entries to the next one") {
    val dir   = newDirectory()
    val first = store(dir)
    first.append(Vector(entry(1), entry(2))).unsafeRun()

    val second = store(dir)
    seqs(second.read(0L).unsafeRun()) shouldBe Vector(1, 2)
    second.close().unsafeRun()
  }

  test("the next store keeps the valid entries of an open file that ends in a cut-off append") {
    val dir   = newDirectory()
    val first = store(dir)
    first.append(Vector(entry(1), entry(2))).unsafeRun()
    first.close().unsafeRun()
    Files.write(dir.resolve("open"), Array[Byte](0x50, 0x4c), StandardOpenOption.APPEND)

    val second = store(dir)
    seqs(second.read(0L).unsafeRun()) shouldBe Vector(1, 2)
    second.close().unsafeRun()
  }

  test("successive batches remain ordered and may contain sequence gaps") {
    val journal = store(newDirectory())
    journal.append(Vector(entry(1), entry(3))).unsafeRun()
    journal.append(Vector(entry(5), entry(8))).unsafeRun()
    seqs(journal.read(0L).unsafeRun()) shouldBe Vector(1, 3, 5, 8)
  }

  test("a sequence at or below the high water is rejected, and the store stays usable") {
    val journal = store(newDirectory())
    journal.append(Vector(entry(1), entry(3))).unsafeRun()
    an[IllegalStateException] should be thrownBy journal.append(Vector(entry(1), entry(3))).unsafeRun()
    an[IllegalStateException] should be thrownBy journal.append(Vector(entry(3, id = 99L))).unsafeRun()
    // A sequence that was skipped cannot be filled in later.
    an[IllegalStateException] should be thrownBy journal.append(Vector(entry(2))).unsafeRun()
    journal.append(Vector(entry(4))).unsafeRun()
    seqs(journal.read(0L).unsafeRun()) shouldBe Vector(1, 3, 4)
  }

  test("the sequences of a batch must increase and must not be negative") {
    val journal = store(newDirectory())
    an[IllegalArgumentException] should be thrownBy journal.append(Vector(entry(2), entry(2))).unsafeRun()
    an[IllegalArgumentException] should be thrownBy journal.append(Vector(entry(3), entry(1))).unsafeRun()
    an[IllegalArgumentException] should be thrownBy journal.append(Vector(entry(-1))).unsafeRun()
    journal.read(0L).unsafeRun() shouldBe empty
    journal.maxSeq.unsafeRun() shouldBe None
  }

  test("maxSeq and maxEnvelopeId reflect the highest seq and the highest id or cause appended") {
    val journal = store(newDirectory())
    journal.append(Vector(entry(2, id = 3L, cause = 1L), entry(9, id = 5L, cause = 9L))).unsafeRun()
    journal.append(Vector(entry(11, id = 7L, cause = 2L))).unsafeRun()
    journal.maxSeq.unsafeRun() shouldBe Some(11L)
    journal.maxEnvelopeId.unsafeRun() shouldBe Some(9L)
  }

  test("the high water is read back from sealed footers after a restart") {
    val dir   = newDirectory()
    val first = store(dir, maxSegmentBytes = sealEveryEntry)
    first.append(Vector(entry(1, id = 3L, cause = 9L), entry(2, id = 5L))).unsafeRun()
    first.close().unsafeRun()

    val second = store(dir)
    second.maxSeq.unsafeRun() shouldBe Some(2L)
    second.maxEnvelopeId.unsafeRun() shouldBe Some(9L)
    second.close().unsafeRun()
  }

  test("an entry larger than maxEntryBytes fails the append before it is written") {
    val journal = store(newDirectory(), maxEntryBytes = 16)
    val error   = intercept[IllegalArgumentException](journal.append(Vector(entry(1))).unsafeRun())
    error.getMessage should include("maxEntrySize")
    journal.read(0L).unsafeRun() shouldBe empty
    journal.close().unsafeRun()
  }

  test("lowering maxEntryBytes does not make stored entries unreadable") {
    val dir    = newDirectory()
    val writer = store(dir, maxEntryBytes = 1024)
    writer.append(Vector(entry(1))).unsafeRun()
    writer.close().unsafeRun()

    val reader = store(dir, maxEntryBytes = 1)
    seqs(reader.read(0L).unsafeRun()) shouldBe Vector(1L)
    reader.close().unsafeRun()
  }

  test("truncate deletes the sealed segments at or below the seq and keeps their high water") {
    val dir     = newDirectory()
    val journal = store(dir, maxSegmentBytes = sealEveryEntry)
    journal.append(Vector(entry(1, id = 3L, cause = 9L), entry(2, id = 4L))).unsafeRun()
    journal.append(Vector(entry(3, id = 5L), entry(4, id = 6L))).unsafeRun()
    names(dir) should not contain "high-water"

    journal.truncate(2L).unsafeRun()
    names(dir) shouldBe Vector("3-3.jrl", "high-water", "open", "running")
    seqs(journal.read(0L).unsafeRun()) shouldBe Vector(3, 4)
    journal.maxSeq.unsafeRun() shouldBe Some(4L)
    journal.maxEnvelopeId.unsafeRun() shouldBe Some(9L)
    journal.close().unsafeRun()

    val recovered = store(dir)
    seqs(recovered.read(0L).unsafeRun()) shouldBe Vector(3, 4)
    recovered.maxSeq.unsafeRun() shouldBe Some(4L)
    recovered.maxEnvelopeId.unsafeRun() shouldBe Some(9L)
    recovered.close().unsafeRun()
  }

  test("the high water outlives every segment, so a deleted sequence cannot be reused after a restart") {
    val dir   = newDirectory()
    val first = store(dir)
    first.append(Vector(entry(1, id = 3L, cause = 9L))).unsafeRun()
    first.close().unsafeRun()

    // Opening the log sealed the segment that held entry 1.
    val second = store(dir)
    second.truncate(1L).unsafeRun()
    second.read(0L).unsafeRun() shouldBe empty
    second.maxSeq.unsafeRun() shouldBe Some(1L)
    second.maxEnvelopeId.unsafeRun() shouldBe Some(9L)
    second.close().unsafeRun()

    val third = store(dir)
    third.read(0L).unsafeRun() shouldBe empty
    third.maxSeq.unsafeRun() shouldBe Some(1L)
    third.maxEnvelopeId.unsafeRun() shouldBe Some(9L)
    an[IllegalStateException] should be thrownBy third.append(Vector(entry(1))).unsafeRun()
    third.append(Vector(entry(2, id = 11L))).unsafeRun()
    third.maxSeq.unsafeRun() shouldBe Some(2L)
    third.maxEnvelopeId.unsafeRun() shouldBe Some(11L)
    third.close().unsafeRun()
  }

  test("truncate keeps the segment being written and stores no high water for it") {
    val dir     = newDirectory()
    val journal = store(dir)
    journal.append(Vector(entry(1), entry(2))).unsafeRun()
    journal.truncate(2L).unsafeRun()
    seqs(journal.read(0L).unsafeRun()) shouldBe Vector(1, 2)
    names(dir) should not contain "high-water"
  }

  test("truncate keeps a sealed segment that holds a later seq") {
    val dir   = newDirectory()
    val first = store(dir)
    first.append(Vector(entry(3), entry(7))).unsafeRun()
    first.close().unsafeRun()

    val second = store(dir)
    second.truncate(5L).unsafeRun()
    seqs(second.read(0L).unsafeRun()) shouldBe Vector(3, 7)
    names(dir) should not contain "high-water"
    second.close().unsafeRun()
  }

  test("successive truncations advance one high-water file") {
    val dir     = newDirectory()
    val journal = store(dir, maxSegmentBytes = sealEveryEntry)
    journal.append(Vector(entry(1, id = 3L), entry(2, id = 7L), entry(3, id = 5L))).unsafeRun()
    journal.truncate(1L).unsafeRun()
    journal.truncate(2L).unsafeRun()
    names(dir) shouldBe Vector("high-water", "open", "running")
    journal.close().unsafeRun()

    val recovered = store(dir)
    seqs(recovered.read(0L).unsafeRun()) shouldBe Vector(3L)
    recovered.maxSeq.unsafeRun() shouldBe Some(3L)
    recovered.maxEnvelopeId.unsafeRun() shouldBe Some(7L)
    recovered.close().unsafeRun()
  }

  test("a damaged high-water file fails the open") {
    val dir     = newDirectory()
    val journal = store(dir, maxSegmentBytes = sealEveryEntry)
    journal.append(Vector(entry(1), entry(2))).unsafeRun()
    journal.truncate(1L).unsafeRun()
    journal.close().unsafeRun()
    val path  = dir.resolve("high-water")
    val bytes = Files.readAllBytes(path)
    bytes(bytes.length - 1) = (bytes.last ^ 0xff).toByte
    Files.write(path, bytes)

    val recovered = unopened(dir)
    an[InvalidChecksumException] should be thrownBy recovered.open().unsafeRun()
    recovered.close().unsafeRun()
  }
