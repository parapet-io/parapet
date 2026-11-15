package io.parapet.journal

import io.parapet.{Codec, ProcessRef}
import java.nio.ByteBuffer
import scala.util.Try

/** A recorded delivery. Application codecs own the entire event payload, including any version information. */
final case class JournalEntry(
    seq: Long,
    id: Long,
    sender: ProcessRef.Unknown,
    receiver: ProcessRef.Unknown,
    cause: Long,
    event: Array[Byte],
    kind: JournalEntry.Kind = JournalEntry.Kind.Event
) derives Codec

object JournalEntry {

  /** Runtime record routing, independent of application payload formats. */
  enum Kind(val id: Int):
    case Event        extends Kind(0)
    case Registration extends Kind(1)

  object Kind {
    def fromId(id: Int): Kind = id match {
      case 0 => Kind.Event
      case 1 => Kind.Registration
      case _ => throw new IllegalArgumentException(s"unknown journal entry kind: $id")
    }

    given Codec[Kind] with {
      def encode(value: Kind): Try[Array[Byte]] = summon[Codec[Int]].encode(value.id)
      def decode(buffer: ByteBuffer): Try[Kind] = Try(fromId(buffer.getInt()))
    }
  }
}
