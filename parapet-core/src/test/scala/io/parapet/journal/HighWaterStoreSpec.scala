package io.parapet.journal

import io.parapet.journal.HighWaterStore.HighWater
import io.parapet.journal.log.{CorruptLogException, InvalidChecksumException}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.nio.ByteBuffer
import java.nio.file.{Files, Path}

class HighWaterStoreSpec extends AnyFunSuite:

  private def newDirectory(): Path = Files.createTempDirectory("high-water-spec")

  private def file(directory: Path): Path = directory.resolve("high-water")

  /** A directory whose store holds `value`. */
  private def stored(value: HighWater): Path =
    val directory = newDirectory()
    new HighWaterStore(directory).advance(value)
    directory

  test("nothing is stored until the first advance") {
    val directory = newDirectory()
    new HighWaterStore(directory).read() shouldBe None
    new HighWaterStore(directory.resolve("absent")).read() shouldBe None
    Files.exists(file(directory)) shouldBe false
  }

  test("advance stores the value, and a new store reads it") {
    val directory = newDirectory()
    val store     = new HighWaterStore(directory)
    store.advance(HighWater(5L, 9L))
    store.read() shouldBe Some(HighWater(5L, 9L))
    new HighWaterStore(directory).read() shouldBe Some(HighWater(5L, 9L))
  }

  test("advance never lowers either number") {
    val directory = stored(HighWater(5L, 9L))
    val store     = new HighWaterStore(directory)
    store.advance(HighWater(7L, 3L))
    store.read() shouldBe Some(HighWater(7L, 9L))
    store.advance(HighWater(1L, 1L))
    store.read() shouldBe Some(HighWater(7L, 9L))
    new HighWaterStore(directory).read() shouldBe Some(HighWater(7L, 9L))
  }

  test("advance creates an absent directory") {
    val directory = newDirectory().resolve("nested").resolve("journal")
    new HighWaterStore(directory).advance(HighWater(1L, 2L))
    new HighWaterStore(directory).read() shouldBe Some(HighWater(1L, 2L))
  }

  test("a temporary file left by a crash is ignored and replaced") {
    val directory = stored(HighWater(5L, 9L))
    val temporary = directory.resolve("high-water.tmp")
    Files.write(temporary, Array[Byte](1, 2, 3))
    val store = new HighWaterStore(directory)
    store.read() shouldBe Some(HighWater(5L, 9L))
    store.advance(HighWater(6L, 9L))
    store.read() shouldBe Some(HighWater(6L, 9L))
    Files.exists(temporary) shouldBe false
  }

  test("a file of the wrong length or with the wrong magic is corrupt") {
    val bytes = Files.readAllBytes(file(stored(HighWater(5L, 9L))))
    val cases = Map(
      "an empty file"   -> Array.emptyByteArray,
      "a cut file"      -> bytes.dropRight(1),
      "trailing bytes"  -> (bytes :+ 0.toByte),
      "the wrong magic" -> withChecksum(ByteBuffer.wrap(bytes.clone()).putInt(0, 0).array())
    )
    for (name, damaged) <- cases do
      val directory = newDirectory()
      Files.write(file(directory), damaged)
      withClue(s"$name: ") {
        val error = intercept[CorruptLogException](new HighWaterStore(directory).read())
        error.getMessage should include(file(directory).toString)
        intercept[CorruptLogException](new HighWaterStore(directory).advance(HighWater(1L, 1L)))
        Files.readAllBytes(file(directory)).toVector shouldBe damaged.toVector
      }
  }

  test("a failed checksum is an InvalidChecksumException") {
    val directory = stored(HighWater(5L, 9L))
    val bytes     = Files.readAllBytes(file(directory))
    for offset <- List(4, 12, bytes.length - 1) do
      val damaged = bytes.clone()
      damaged(offset) = (damaged(offset) ^ 1).toByte
      Files.write(file(directory), damaged)
      withClue(s"byte $offset: ")(intercept[InvalidChecksumException](new HighWaterStore(directory).read()))
  }

  test("negative numbers are corrupt") {
    val directory = stored(HighWater(5L, 9L))
    val bytes     = Files.readAllBytes(file(directory))
    for offset <- List(4, 12) do
      Files.write(file(directory), withChecksum(ByteBuffer.wrap(bytes.clone()).putLong(offset, -1L).array()))
      withClue(s"number at $offset: ")(intercept[CorruptLogException](new HighWaterStore(directory).read()))
  }

  /** `bytes` with the checksum of its first 20 bytes in its last 4. */
  private def withChecksum(bytes: Array[Byte]): Array[Byte] =
    ByteBuffer.wrap(bytes).putInt(20, HighWaterStore.checksum(bytes.take(20))).array()
