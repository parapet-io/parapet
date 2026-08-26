package io.parapet.journal

import io.parapet.TestUtils.{*, given}
import io.parapet.journal.{JournalEntry, JournalMetadata, JournalSegment, JournalStoreLocal}
import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

class JournalStoreLocalSpec extends AnyFunSuite:

  private val a = ProcessRef[Event]("a")
  private val b = ProcessRef[Event]("b")

  private def entry(seq: Long, receiver: ProcessRef.Unknown = a, id: Long = 0L, cause: Long = 0L) =
    JournalEntry(seq, id, a, receiver, cause, s"e$seq".getBytes(UTF_8), tag = "e", schemaVersion = 1)

  private def seg(entries: JournalEntry*): JournalSegment =
    val v = entries.toVector
    JournalSegment(JournalMetadata.of(v, createdAt = 0L), v)

  private def newStore(dir: Path = Files.createTempDirectory("journal-spec")) =
    (dir, new JournalStoreLocal[TestIO](JournalStoreLocal.Config(dir)))

  private def seqs(entries: Vector[JournalEntry]): Vector[Long] = entries.map(_.seq)

  test("append then read returns entries after the given seq, sorted") {
    val (_, store) = newStore()
    store.append(seg(entry(1), entry(2), entry(3))).unsafeRun()
    seqs(store.read(0L).unsafeRun()) shouldBe Vector(1, 2, 3)
    seqs(store.read(1L).unsafeRun()) shouldBe Vector(2, 3)
    store.read(3L).unsafeRun() shouldBe empty
  }

  test("read merges and sorts across batches, including out-of-order ranges") {
    val (_, store) = newStore()
    store.append(seg(entry(5), entry(7))).unsafeRun()
    store.append(seg(entry(3), entry(4), entry(6))).unsafeRun()
    seqs(store.read(0L).unsafeRun()) shouldBe Vector(3, 4, 5, 6, 7)
    seqs(store.read(4L).unsafeRun()) shouldBe Vector(5, 6, 7)
  }

  test("maxSeq reflects the highest stored seq") {
    val (_, store) = newStore()
    store.maxSeq.unsafeRun() shouldBe None
    store.append(seg(entry(2), entry(9))).unsafeRun()
    store.append(seg(entry(4))).unsafeRun()
    store.maxSeq.unsafeRun() shouldBe Some(9L)
  }

  test("maxEnvelopeId reflects the highest id or cause stored") {
    val (_, store) = newStore()
    store.maxEnvelopeId.unsafeRun() shouldBe None
    store.append(seg(entry(1, id = 3L, cause = 1L), entry(2, id = 5L, cause = 9L))).unsafeRun() // cause 9 is highest
    store.append(seg(entry(3, id = 7L, cause = 2L))).unsafeRun()
    store.maxEnvelopeId.unsafeRun() shouldBe Some(9L)
  }

  test("truncate drops segments fully at or below upToSeq, keeps the rest") {
    val (_, store) = newStore()
    store.append(seg(entry(1), entry(2))).unsafeRun() // [1,2]
    store.append(seg(entry(3), entry(4))).unsafeRun() // [3,4]
    store.append(seg(entry(5), entry(6))).unsafeRun() // [5,6]
    store.truncate(4L).unsafeRun()
    seqs(store.read(0L).unsafeRun()) shouldBe Vector(5, 6)
  }

  test("truncate keeps a segment straddling upToSeq whole") {
    val (_, store) = newStore()
    store.append(seg(entry(3), entry(7))).unsafeRun() // [3,7]
    store.truncate(5L).unsafeRun()                    // maxSeq 7 > 5, keep
    seqs(store.read(0L).unsafeRun()) shouldBe Vector(3, 7)
  }

  test("an empty segment writes nothing") {
    val (_, store) = newStore()
    store.append(JournalSegment(JournalMetadata(0L, 0L, 0, 0L, 0L), Vector.empty)).unsafeRun()
    store.read(0L).unsafeRun() shouldBe empty
    store.maxSeq.unsafeRun() shouldBe None
  }

  test("a new store over the same directory sees existing segments") {
    val (dir, first) = newStore()
    first.append(seg(entry(1), entry(2))).unsafeRun()

    val (_, second) = newStore(dir)
    seqs(second.read(0L).unsafeRun()) shouldBe Vector(1, 2)
    second.maxSeq.unsafeRun() shouldBe Some(2L)
  }

  test("read preserves receiver and payload") {
    val (_, store) = newStore()
    store.append(seg(entry(1, receiver = b))).unsafeRun()
    val loaded = store.read(0L).unsafeRun().head
    loaded.receiver shouldBe b
    new String(loaded.event, UTF_8) shouldBe "e1"
  }

  test("read of a non-existent data dir is empty (cold start)") {
    val absent     = Files.createTempDirectory("journal-cold").resolve("nope")
    val (_, store) = newStore(absent)
    store.read(0L).unsafeRun() shouldBe empty
    store.maxSeq.unsafeRun() shouldBe None
  }

  test("read fails fast when the data dir path is not a directory") {
    val file       = Files.createTempFile("journal-not-a-dir", ".tmp")
    val (_, store) = newStore(file)
    an[java.io.IOException] should be thrownBy store.read(0L).unsafeRun()
  }

  test("a malformed segment filename fails loud, not silently skipped") {
    val (dir, store) = newStore()
    store.append(seg(entry(1))).unsafeRun()
    Files.write(dir.resolve("garbage.jrnl"), "x".getBytes(UTF_8))
    an[IllegalStateException] should be thrownBy store.maxSeq.unsafeRun()
    an[IllegalStateException] should be thrownBy store.read(0L).unsafeRun()
  }
