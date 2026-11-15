package io.parapet.journal

import io.parapet.Event.Registered
import io.parapet.Codec
import java.nio.ByteBuffer
import scala.util.Try

/** Internal payload codec for registration records, routed by JournalEntry.Kind. */
private[parapet] object RegisteredEventCodec extends Codec[Registered] {
  private val codec: Codec[Registered]            = Codec.derived
  def encode(value: Registered): Try[Array[Byte]] = codec.encode(value)
  def decode(buffer: ByteBuffer): Try[Registered] = codec.decode(buffer)
}
