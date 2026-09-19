package io.parapet.journal

import java.nio.ByteBuffer
import java.nio.channels.{ClosedChannelException, NonWritableChannelException, SeekableByteChannel}

final private[journal] class InMemorySeekableByteChannel(
    bytes: Array[Byte],
    maxReadSize: Int = Int.MaxValue
) extends SeekableByteChannel:

  require(maxReadSize > 0, "maxReadSize must be positive")

  private val data   = bytes.clone()
  private var offset = 0L
  private var open   = true

  override def read(destination: ByteBuffer): Int =
    ensureOpen()
    if offset >= data.length then -1
    else
      val count = math.min(math.min(destination.remaining(), maxReadSize), data.length - offset.toInt)
      destination.put(data, offset.toInt, count)
      offset += count
      count

  override def write(source: ByteBuffer): Int =
    ensureOpen()
    throw new NonWritableChannelException()

  override def position(): Long =
    ensureOpen()
    offset

  override def position(newPosition: Long): SeekableByteChannel =
    ensureOpen()
    if newPosition < 0L then throw new IllegalArgumentException(s"negative position: $newPosition")
    offset = newPosition
    this

  override def size(): Long =
    ensureOpen()
    data.length.toLong

  override def truncate(size: Long): SeekableByteChannel =
    ensureOpen()
    throw new NonWritableChannelException()

  override def isOpen: Boolean = open

  override def close(): Unit = open = false

  private def ensureOpen(): Unit =
    if !open then throw new ClosedChannelException()
