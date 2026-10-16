package io.parapet

import scala.util.{Failure, Try}
import io.parapet.EventCodec.{EncodedEvent, Schema}

/** A codec usable by the runtime. Implement [[EventCodec.Simple]] or compose codecs with [[EventCodec.union]]. */
trait EventCodec[A <: Event] {
  def schema: Schema
  private[parapet] def encodeEvent(value: A): Try[EncodedEvent]

  /** Reads supported historical versions into the current domain model. */
  private[parapet] def decodeEvent(encoded: EncodedEvent): Try[A]
}

object EventCodec {

  /** Stable wire identities supported by a codec. Union order and nesting do not affect routing. */
  sealed trait Schema {
    def leaves: Set[Schema.Simple]
    final def contains(schema: Schema.Simple): Boolean = leaves.contains(schema)
  }

  object Schema {

    /** A stable payload identity independent of Scala class names.
      */
    final case class Simple(namespace: String, name: String) extends Schema {
      def leaves: Set[Simple] = Set(this)
    }

    final case class Union(first: Schema, second: Schema) extends Schema {
      require(first.leaves.intersect(second.leaves).isEmpty, "Duplicate event schemas in union")
      val leaves: Set[Simple] = first.leaves ++ second.leaves
    }
  }

  /** One payload and the exact leaf identity and format version used to encode it. */
  final private[parapet] case class EncodedEvent(schema: Schema.Simple, version: Int, bytes: Array[Byte])

  /** Client implementation of one schema identity, with independently versioned payloads. */
  abstract class Simple[A <: Event] extends EventCodec[A] {
    def schema: Schema.Simple

    /** Version written by encode. Decode may also support historical versions. */
    def version: Int
    def encode(value: A): Try[Array[Byte]]
    def decode(version: Int, bytes: Array[Byte]): Try[A]

    final private[parapet] def encodeEvent(value: A): Try[EncodedEvent] =
      encode(value).map(bytes => EncodedEvent(schema, version, bytes))

    final private[parapet] def decodeEvent(encoded: EncodedEvent): Try[A] =
      if encoded.schema != schema then
        Failure(new IllegalArgumentException(s"Unsupported event schema: ${encoded.schema}"))
      else decode(encoded.version, encoded.bytes)
  }

  /** Compose disjoint schemas with explicit typed routing for encoding. */
  def union[A <: Event, B <: Event](first: EventCodec[A], second: EventCodec[B])(
      select: (A | B) => Either[A, B]
  ): EventCodec[A | B] = {
    val combined = Schema.Union(first.schema, second.schema)
    new EventCodec[A | B] {
      val schema: Schema                                                = combined
      private[parapet] def encodeEvent(value: A | B): Try[EncodedEvent] =
        Try(select(value)).flatMap {
          case Left(a)  => first.encodeEvent(a)
          case Right(b) => second.encodeEvent(b)
        }
      private[parapet] def decodeEvent(encoded: EncodedEvent): Try[A | B] =
        if first.schema.contains(encoded.schema) then first.decodeEvent(encoded)
        else if second.schema.contains(encoded.schema) then second.decodeEvent(encoded)
        else Failure(new IllegalArgumentException(s"Unsupported event schema: ${encoded.schema}"))
    }
  }
}
