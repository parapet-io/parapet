package io.parapet.tests.intg.pario

import io.parapet.core.journal.*
import io.parapet.effect.ParIO
import io.parapet.effect.ParIO.given
import io.parapet.{Event, ProcessRef}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import scala.concurrent.duration.*

class JournalManagerIntgSpec extends AnyFunSuite:

  private val ref = ProcessRef[Event]("a")

  extension [A](fa: ParIO[A]) private def run(): A = fa.unsafeRunSync()

  private def entry(seq: Long) = JournalEntry(seq, ref, ref, 0L, s"e$seq".getBytes(UTF_8))

  private def storeAt(dir: Path) = new JournalStoreLocal[ParIO](JournalStoreLocal.Config(dir))

  private def seqsOnDisk(dir: Path): Vector[Long] = storeAt(dir).read(0L).run().map(_.seq)

  test("the time trigger flushes buffered entries below batchSize, without close") {
    val dir     = Files.createTempDirectory("jrnl-mgr-time")
    val manager = JournalManager[ParIO](storeAt(dir), JournalConfig(batchSize = 100, flushInterval = 50.millis)).run()
    try
      (1 to 3).foreach(i => manager.append(entry(i.toLong)).run())
      Thread.sleep(300) // let the timer fire; batchSize (100) is not reached and close is not called
      seqsOnDisk(dir) shouldBe Vector(1, 2, 3)
    finally manager.close.run()
  }

  test("the count trigger flushes at batchSize, without the timer or close") {
    val dir     = Files.createTempDirectory("jrnl-mgr-batch")
    val manager = JournalManager[ParIO](storeAt(dir), JournalConfig(batchSize = 3, flushInterval = 1.hour)).run()
    try
      (1 to 3).foreach(i => manager.append(entry(i.toLong)).run())
      Thread.sleep(300)
      seqsOnDisk(dir) shouldBe Vector(1, 2, 3)
    finally manager.close.run()
  }

  test("close flushes whatever remains in the buffer") {
    val dir     = Files.createTempDirectory("jrnl-mgr-close")
    val manager = JournalManager[ParIO](storeAt(dir), JournalConfig(batchSize = 100, flushInterval = 1.hour)).run()
    (1 to 2).foreach(i => manager.append(entry(i.toLong)).run())
    manager.close.run()
    seqsOnDisk(dir) shouldBe Vector(1, 2)
  }
