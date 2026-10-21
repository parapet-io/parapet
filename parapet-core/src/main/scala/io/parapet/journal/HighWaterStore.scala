package io.parapet.journal

import io.parapet.journal.log.{CorruptLogException, InvalidChecksumException}

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardCopyOption, StandardOpenOption}
import java.util.zip.CRC32C

/** The highest delivery sequence and envelope ID ever recorded, kept in the file `high-water` in `directory`. Segment
  * footers prove these values while the segments exist, so a journal stores them here just before it deletes segments.
  * The stored value only grows. One instance may be shared by the journals of a directory; two instances for one
  * directory must not be used at the same time.
  *
  * {{{
  * [magic: 4][delivery sequence: 8][envelope ID: 8][checksum: 4]
  * }}}
  */
final private[parapet] class HighWaterStore(directory: Path):
  import HighWaterStore.*

  private val path = directory.resolve("high-water")

  /** The stored value, or None when none has been stored. A damaged file is a CorruptLogException, an
    * InvalidChecksumException for a failed checksum.
    */
  def read(): Option[HighWater] = synchronized {
    Option.when(Files.exists(path))(decode(Files.readAllBytes(path)))
  }

  /** Raises each stored number to at least the one in `value`, and forces the result to storage before it returns. */
  def advance(value: HighWater): Unit = synchronized {
    val stored = read()
    val merged = stored.fold(value)(_.merge(value))
    if !stored.contains(merged) then write(merged)
  }

  /** Replaces the file in one step, so a crash leaves the old value or the new one. */
  private def write(value: HighWater): Unit =
    Files.createDirectories(directory)
    val temporary = directory.resolve("high-water.tmp")
    val channel   = FileChannel.open(
      temporary,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.WRITE
    )
    try
      FileUtils.writeFully(channel, ByteBuffer.wrap(encode(value)))
      channel.force(true)
    finally channel.close()
    Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
    FileUtils.forceDirectoryChain(directory)

  private def encode(value: HighWater): Array[Byte] =
    val bytes = ByteBuffer.allocate(EncodedSize).putInt(Magic).putLong(value.deliverySeq).putLong(value.envelopeId)
    bytes.putInt(checksum(bytes.array().take(ChecksumOffset))).array()

  private def decode(bytes: Array[Byte]): HighWater =
    if bytes.length != EncodedSize then throw corrupt(s"${bytes.length} bytes instead of $EncodedSize")
    val buffer = ByteBuffer.wrap(bytes)
    if buffer.getInt() != Magic then throw corrupt("wrong magic")
    val value = HighWater(buffer.getLong(), buffer.getLong())
    if buffer.getInt() != checksum(bytes.take(ChecksumOffset)) then
      throw new InvalidChecksumException(s"invalid high-water file $path: invalid checksum")
    if value.deliverySeq < 0 || value.envelopeId < 0 then throw corrupt(s"negative value in $value")
    value

  private def corrupt(message: String): CorruptLogException =
    new CorruptLogException(s"invalid high-water file $path: $message")

object HighWaterStore:
  final case class HighWater(deliverySeq: Long, envelopeId: Long):
    def merge(other: HighWater): HighWater =
      HighWater(math.max(deliverySeq, other.deliverySeq), math.max(envelopeId, other.envelopeId))

  private val Magic          = 0x50454831
  private val ChecksumOffset = Integer.BYTES + java.lang.Long.BYTES * 2
  private val EncodedSize    = ChecksumOffset + Integer.BYTES

  private[journal] def checksum(bytes: Array[Byte]): Int =
    val crc = new CRC32C
    crc.update(bytes)
    crc.getValue.toInt
