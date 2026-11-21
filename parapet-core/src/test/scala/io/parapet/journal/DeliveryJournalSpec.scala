package io.parapet.journal

import io.parapet.TestUtils.{TestIO, given}
import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** The delivery journal on a [[JournalStoreLocal]]: what reaches the files, and when. */
class DeliveryJournalSpec extends AnyFunSuite:

  private val ref = ProcessRef[Event]("a")

  private def draft(id: Long): JournalDraft =
    JournalDraft(id, ref, ref, 0L, Array[Byte](id.toByte))

  private def names(dir: Path): Vector[String] =
    val listing = Files.list(dir)
    try listing.iterator().asScala.map(_.getFileName.toString).toVector.sorted
    finally listing.close()

  /** Opens `store`, as the runtime does, and runs `use` on a journal whose writer runs on a thread of its own. Then
    * closes the journal and checks that the writer ended without a failure.
    */
  private def recording(store: JournalStore[TestIO])(use: DeliveryJournal[TestIO] => Unit): Unit =
    store.open().unsafeRun()
    val journal = DeliveryJournal.fresh(store, JournalConfig(batchSize = 4))
    val outcome = new AtomicReference[Try[Unit]]()
    val writer  = new Thread(() => outcome.set(Try(journal.runWriter.unsafeRun())), "delivery-journal-writer")
    writer.start()
    try
      use(journal)
      journal.close().unsafeRun()
    finally
      writer.join(TimeUnit.SECONDS.toMillis(5L))
      if writer.isAlive then writer.interrupt()
    writer.isAlive shouldBe false
    outcome.get().get

  test("close publishes the admitted deliveries, then closes the store, which records a clean stop") {
    val dir = Files.createTempDirectory("delivery-journal-spec")
    recording(new JournalStoreLocal[TestIO](JournalStoreLocal.Config(dir))) { journal =>
      journal.admit(draft(1L)).unsafeRun() shouldBe 1L
      journal.admit(draft(2L)).unsafeRun() shouldBe 2L
    }
    // No running marker is left.
    names(dir) shouldBe Vector("open")

    val reopened = new JournalStoreLocal[TestIO](JournalStoreLocal.Config(dir))
    reopened.open().unsafeRun()
    reopened.read(0L).unsafeRun().map(_.seq) shouldBe Vector(1L, 2L)
    reopened.maxSeq.unsafeRun() shouldBe Some(2L)
    reopened.close().unsafeRun()
  }

  test("truncate publishes the buffered tail before deleting the sealed segments it covers") {
    val dir = Files.createTempDirectory("delivery-journal-spec")
    // With a limit of one byte every entry seals the one before it, and truncate deletes sealed segments only.
    val store = new JournalStoreLocal[TestIO](JournalStoreLocal.Config(dir, maxSegmentBytes = 1L))
    recording(store) { journal =>
      journal.admit(draft(1L)).unsafeRun() shouldBe 1L
      journal.admit(draft(2L)).unsafeRun() shouldBe 2L
      store.read(0L).unsafeRun() shouldBe empty

      journal.truncate(1L).unsafeRun()
      journal.read(0L).unsafeRun().map(_.seq) shouldBe Vector(2L)
      journal.maxSeq.unsafeRun() shouldBe Some(2L)

      journal.admit(draft(3L)).unsafeRun() shouldBe 3L
    }
    names(dir) shouldBe Vector("2-2.jrl", "high-water", "open")
  }
