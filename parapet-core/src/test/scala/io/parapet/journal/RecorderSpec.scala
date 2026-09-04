package io.parapet.journal

import io.parapet.TestUtils.{TestIO, given}
import io.parapet.journal.Recorder.{Config, Entry, Store}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import scala.collection.mutable.ListBuffer

class RecorderSpec extends AnyFunSuite:

  private def fixture(batchSize: Int): (Recorder[TestIO, String], ListBuffer[Vector[Entry[String]]]) =
    val appended = ListBuffer.empty[Vector[Entry[String]]]
    val store    = new Store[TestIO, String]:
      def append(entries: Vector[Entry[String]]): TestIO[Unit] =
        TestIO.delay(appended += entries).map(_ => ())

    (Recorder(store, Config(startId = 0L, batchSize = batchSize)), appended)

  test("admitAndFlush seals the active batch through the admitted entry") {
    val (recorder, appended) = fixture(batchSize = 4)

    recorder.admit("a").unsafeRun() shouldBe 1L
    recorder.admit("b").unsafeRun() shouldBe 2L
    appended shouldBe empty

    recorder.admitAndFlush("c").unsafeRun() shouldBe 3L

    appended.toList shouldBe List(Vector(Entry(1L, "a"), Entry(2L, "b"), Entry(3L, "c")))
  }

  test("admitAndFlush stores a singleton batch when the active batch is empty") {
    val (recorder, appended) = fixture(batchSize = 4)

    recorder.admitAndFlush("a").unsafeRun() shouldBe 1L

    appended.toList shouldBe List(Vector(Entry(1L, "a")))
  }
