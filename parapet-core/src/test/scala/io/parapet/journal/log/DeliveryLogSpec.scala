package io.parapet.journal.log

import io.parapet.{Event, ProcessRef}
import io.parapet.EventCodec.Schema
import io.parapet.journal.{HighWaterStore, JournalDurability, JournalEntry}
import io.parapet.journal.HighWaterStore.HighWater
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

class DeliveryLogSpec extends AnyFunSuite:

  private val process = ProcessRef[Event]("a")

  private def entry(seq: Long, id: Long, cause: Long = 0L): JournalEntry =
    JournalEntry(seq, id, process, process, cause, s"e$seq".getBytes(UTF_8), Schema.Simple("test", "e"), 1)

  private def newDirectory(): Path = Files.createTempDirectory("delivery-log-spec")

  /** Opens the log in `dir`. With a limit of one byte, every entry seals the one before it. */
  private def open(dir: Path, maxSegmentSize: Long = 1L): DeliveryLog =
    val log = new DeliveryLog(
      Log.Config(dir, maxSegmentSize, JournalDurability.ProcessCrash),
      new HighWaterStore(dir)
    )
    log.init()
    log

  private def sealedNames(log: DeliveryLog): List[String] =
    log.readFooters().map((path, _) => path.getFileName.toString)

  test("a segment is named by its sequence range, and its header and footer describe its entries") {
    val dir   = newDirectory()
    val first = open(dir, maxSegmentSize = 1024L * 1024L)
    List(entry(3L, id = 7L), entry(4L, id = 2L, cause = 9L), entry(6L, id = 5L)).foreach(first.append(_).get)
    first.close()
    // The next init seals the segment.
    val log = open(dir)
    try
      val List((path, footer)) = log.readFooters(): @unchecked
      path shouldBe dir.resolve("3-6.jrl")
      // The highest ID counts the IDs of the entries and of their causes.
      footer.footer shouldBe DeliveryLog.Footer(minSeq = 3L, maxSeq = 6L, maxId = 9L)
      val segment = log.readSegment(path).get
      segment.header shouldBe DeliveryLog.Header(id = 7L, seq = 3L)
      segment.entries.map(_.seq) shouldBe List(3L, 4L, 6L)
      new String(segment.entries.head.event, UTF_8) shouldBe "e3"
    finally log.close()
  }

  test("read returns the entries above a sequence from the sealed segments and the active one") {
    val dir = newDirectory()
    val log = open(dir)
    try
      log.read(0L) shouldBe empty
      (1L to 4L).foreach(seq => log.append(entry(seq, id = seq)).get)
      sealedNames(log) shouldBe List("1-1.jrl", "2-2.jrl", "3-3.jrl")
      log.read(0L).map(_.seq) shouldBe Vector(1L, 2L, 3L, 4L)
      log.read(2L).map(_.seq) shouldBe Vector(3L, 4L)
      log.read(3L).map(_.seq) shouldBe Vector(4L)
      log.read(4L) shouldBe empty
    finally log.close()
  }

  test("the high water counts the active segment, the sealed footers and what truncate stored") {
    val dir = newDirectory()
    val log = open(dir)
    try
      log.highWater() shouldBe None
      log.append(entry(1L, id = 10L)).get
      log.highWater() shouldBe Some(HighWater(deliverySeq = 1L, envelopeId = 10L))
      log.append(entry(2L, id = 5L, cause = 40L)).get
      log.highWater() shouldBe Some(HighWater(deliverySeq = 2L, envelopeId = 40L))
      log.append(entry(3L, id = 6L)).get
      log.truncate(2L)
      sealedNames(log) shouldBe empty
      log.highWater() shouldBe Some(HighWater(deliverySeq = 3L, envelopeId = 40L))
    finally log.close()
    val reopened = open(dir)
    try reopened.highWater() shouldBe Some(HighWater(deliverySeq = 3L, envelopeId = 40L))
    finally reopened.close()
  }

  test("truncate deletes the sealed segments at or below the sequence and stores their high water") {
    val dir = newDirectory()
    val log = open(dir)
    try
      List(entry(1L, id = 10L), entry(2L, id = 40L), entry(3L, id = 20L), entry(4L, id = 30L))
        .foreach(log.append(_).get)
      sealedNames(log) shouldBe List("1-1.jrl", "2-2.jrl", "3-3.jrl")
      log.truncate(2L)
      sealedNames(log) shouldBe List("3-3.jrl")
      new HighWaterStore(dir).read() shouldBe Some(HighWater(deliverySeq = 2L, envelopeId = 40L))
      // The segment that holds entry 4 is still active, so it stays.
      log.truncate(9L)
      sealedNames(log) shouldBe empty
      new HighWaterStore(dir).read() shouldBe Some(HighWater(deliverySeq = 3L, envelopeId = 40L))
    finally log.close()
  }

  test("truncate keeps a segment that holds a later sequence, and stores nothing when it deletes nothing") {
    val dir   = newDirectory()
    val first = open(dir, maxSegmentSize = 1024L * 1024L)
    List(entry(3L, id = 1L), entry(7L, id = 2L)).foreach(first.append(_).get)
    first.close()
    val log = open(dir)
    try
      log.truncate(5L)
      sealedNames(log) shouldBe List("3-7.jrl")
      new HighWaterStore(dir).read() shouldBe None
    finally log.close()
  }
