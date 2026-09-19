package io.parapet.journal

import io.parapet.journal.DeliveryLog.writeFully
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel

class DeliveryLogWriterSpec extends AnyFunSuite:

  test("writeFully completes across partial channel writes") {
    val channel = new PartialWritableChannel(maxWriteSize = 2)

    writeFully(channel, ByteBuffer.wrap(Array[Byte](1, 2, 3, 4, 5)))

    channel.bytes shouldBe Vector[Byte](1, 2, 3, 4, 5)
  }

  test("writeFully fails when the channel makes no progress") {
    val channel = new PartialWritableChannel(maxWriteSize = 0)

    val error = the[IOException] thrownBy writeFully(channel, ByteBuffer.wrap(Array[Byte](1)))

    error.getMessage should include("made no progress")
  }

  final private class PartialWritableChannel(maxWriteSize: Int) extends WritableByteChannel:
    private val written = Vector.newBuilder[Byte]
    private var open    = true

    def bytes: Vector[Byte] = written.result()

    override def write(source: ByteBuffer): Int =
      if !open then throw new java.nio.channels.ClosedChannelException()
      val count = math.min(source.remaining(), maxWriteSize)
      var index = 0
      while index < count do
        written += source.get()
        index += 1
      count

    override def isOpen: Boolean = open

    override def close(): Unit = open = false
