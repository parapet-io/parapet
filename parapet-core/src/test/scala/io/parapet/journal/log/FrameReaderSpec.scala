package io.parapet.journal.log

import io.parapet.journal.InMemorySeekableByteChannel
import io.parapet.journal.log.FrameReader.Error
import io.parapet.journal.log.SegmentFormat.FrameKind
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.{NonWritableChannelException, SeekableByteChannel}
import java.util.zip.CRC32C

class FrameReaderSpec extends AnyFunSuite:
  private val headerMagic = 0x504c4844
  private val entryMagic  = 0x504c5243
  private val footerMagic = 0x504c4654
  private val endMagic    = 0x504c454e

  /** What a read returned, with the payload as a Vector so that results compare by value. */
  private type Result = Either[Error, (FrameKind, Vector[Byte])]

  private def reader(bytes: Array[Byte], maxReadSize: Int = Int.MaxValue): FrameReader =
    FrameReader(InMemorySeekableByteChannel(bytes, maxReadSize))

  /** A channel that reports `reportedSize` bytes but answers every read with `readResult`. */
  private def brokenChannel(reportedSize: Long, readResult: Int): SeekableByteChannel = new SeekableByteChannel:
    private var offset                                   = 0L
    def read(destination: ByteBuffer): Int               = readResult
    def write(source: ByteBuffer): Int                   = throw new NonWritableChannelException
    def position(): Long                                 = offset
    def position(newPosition: Long): SeekableByteChannel = { offset = newPosition; this }
    def size(): Long                                     = reportedSize
    def truncate(size: Long): SeekableByteChannel        = throw new NonWritableChannelException
    def isOpen: Boolean                                  = true
    def close(): Unit                                    = ()

  private def read(frames: FrameReader): Result = frames.read().map(frame => frame.kind -> frame.data.toVector)

  /** Reads until an Error: the frames, then the Error that ended them. */
  private def readAll(frames: FrameReader): List[Result] =
    read(frames) match
      case frame @ Right(_) => frame :: readAll(frames)
      case error            => List(error)

  private def readAll(bytes: Array[Byte]): List[Result] = readAll(reader(bytes))

  private def crc(bytes: Array[Byte]): Int =
    val value = new CRC32C
    value.update(bytes)
    value.getValue.toInt

  private def ints(values: Int*): Array[Byte] =
    val buffer = ByteBuffer.allocate(values.size * 4)
    values.foreach(buffer.putInt)
    buffer.array()

  // [magic:4][payload length:4][header CRC:4][payload:N][payload CRC:4]
  private def frame(magic: Int, payload: Array[Byte]): Array[Byte] =
    val frameHeader = ints(magic, payload.length)
    frameHeader ++ ints(crc(frameHeader)) ++ payload ++ ints(crc(payload))

  private def flipped(bytes: Array[Byte], offset: Int): Array[Byte] =
    val damaged = bytes.clone()
    damaged(offset) = (damaged(offset) ^ 1).toByte
    damaged

  private val payloads = List(ints(1, 2), ints(3), ints(4, 5, 6))
  private val encoded  = List(headerMagic, entryMagic, footerMagic).zip(payloads).map(frame(_, _))
  private val file     = encoded.reduce(_ ++ _)
  private val starts   = encoded.scanLeft(0)(_ + _.length)

  private val results: List[Result] =
    List(FrameKind.Header, FrameKind.Entry, FrameKind.Footer)
      .zip(payloads)
      .map((kind, data) => Right(kind -> data.toVector))

  test("reads each frame with its kind and payload, and moves past it"):
    val frames = reader(file)
    frames.size shouldBe file.length
    frames.position shouldBe 0L
    for (result, end) <- results.zip(starts.tail) do
      read(frames) shouldBe result
      frames.position shouldBe end
    frames.position shouldBe frames.size

  test("the end of the file is Incomplete"):
    readAll(file) shouldBe results :+ Left(Error.Incomplete)
    readAll(Array.emptyByteArray) shouldBe List(Left(Error.Incomplete))

  test("partial channel reads return the same frames"):
    readAll(reader(file, maxReadSize = 1)) shouldBe readAll(file)

  test("every cut through a file is Incomplete at the frame it cuts"):
    for cut <- 0 until file.length do
      val complete = starts.tail.count(_ <= cut)
      val frames   = reader(file.take(cut))
      withClue(s"cut at $cut: "):
        readAll(frames) shouldBe results.take(complete) :+ Left(Error.Incomplete)
        frames.position shouldBe starts(complete)

  test("a frame header that fails its checksum is InvalidChecksum"):
    // Bytes that never reached storage before a host crash may read back as zeros.
    readAll(file ++ new Array[Byte](20)).last shouldBe Left(Error.InvalidChecksum)
    // The magic, the payload length and the stored checksum of the second frame.
    for offset <- List(0, 4, 8) do
      withClue(s"frame header byte $offset: "):
        readAll(flipped(file, starts(1) + offset)) shouldBe List(results.head, Left(Error.InvalidChecksum))

  test("a payload that fails its checksum is InvalidChecksum"):
    // The payload of the second frame, then its stored checksum.
    for offset <- List(12, 16) do
      withClue(s"frame byte $offset: "):
        readAll(flipped(file, starts(1) + offset)) shouldBe List(results.head, Left(Error.InvalidChecksum))

  test("an Error leaves the position at the damaged frame, and reading again returns the same Error"):
    for bytes <- List(file.take(starts(1) + 5), file.take(starts(2) + 14), flipped(file, starts(2) + 12)) do
      val frames = reader(bytes)
      val error  = readAll(frames).last
      val at     = frames.position
      withClue(s"${bytes.length} bytes: "):
        starts should contain(at.toInt)
        read(frames) shouldBe error
        frames.position shouldBe at

  test("the position can be set to where a frame starts"):
    val frames = reader(file)
    frames.position(starts(2))
    read(frames) shouldBe results(2)
    frames.position(starts(1))
    read(frames) shouldBe results(1)
    frames.position shouldBe starts(2)
    frames.position(frames.size)
    read(frames) shouldBe Left(Error.Incomplete)

  test("a position inside a frame reads as a damaged frame"):
    val frames = reader(file)
    frames.position(starts(1) + 1)
    read(frames) shouldBe Left(Error.InvalidChecksum)
    frames.position shouldBe starts(1) + 1

  test("a position outside the file is rejected"):
    val frames = reader(file)
    intercept[IllegalArgumentException](frames.position(-1L))
    intercept[IllegalArgumentException](frames.position(frames.size + 1))
    frames.position shouldBe 0L

  test("the trailer says where the footer frame starts"):
    val frames = reader(file ++ ints(encoded.last.length, endMagic))
    frames.readTrailer() shouldBe Some(starts(2).toLong)
    frames.position shouldBe 0L

  test("a file that does not end with a trailer has none"):
    val cases = Map(
      "an empty file"                            -> Array.emptyByteArray,
      "a file shorter than a trailer"            -> ints(endMagic),
      "frames only"                              -> file,
      "a wrong end magic"                        -> (file ++ ints(encoded.last.length, endMagic + 1)),
      "a footer length of zero"                  -> (file ++ ints(0, endMagic)),
      "a footer length without a payload"        -> (file ++ ints(16, endMagic)),
      "a negative footer length"                 -> (file ++ ints(-1, endMagic)),
      "a footer length that is longer than file" -> (file ++ ints(file.length + 1, endMagic))
    )
    for (name, bytes) <- cases do withClue(s"$name: ")(reader(bytes).readTrailer() shouldBe None)

  test("bytes a crash cannot leave throw CorruptLogException"):
    val negativeLength = ints(entryMagic, -1)
    val cases          = Map(
      "an unknown frame magic"    -> frame(0x12345678, ints(1)),
      "a negative payload length" -> (negativeLength ++ ints(crc(negativeLength), 0)),
      "an empty payload"          -> frame(entryMagic, Array.emptyByteArray)
    )
    for (name, bytes) <- cases do withClue(s"$name: ")(intercept[CorruptLogException](readAll(encoded.head ++ bytes)))

  test("a channel that ends before its size is corrupt"):
    intercept[CorruptLogException](FrameReader(brokenChannel(reportedSize = 100L, readResult = -1)).read())

  test("a channel that makes no progress fails with IOException, not corruption"):
    val error = intercept[IOException](FrameReader(brokenChannel(reportedSize = 100L, readResult = 0)).read())
    error should not be a[CorruptLogException]
