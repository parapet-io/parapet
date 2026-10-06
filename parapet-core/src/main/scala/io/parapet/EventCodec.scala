package io.parapet

import scala.util.Try

/** A versioned codec for a process input protocol. One instance covers every business-event variant in `A`.
  */
trait EventCodec[A <: Event] extends Codec[A] {
  import EventCodec.*

  /** Stable identifier for this protocol. It must not change while previously encoded events may be replayed. */
  def tag: Tag

  /** Schema version written by [[encode]]. */
  def version: Int

  /** Decodes an event written with `version`. Supported historical versions are mapped to the current event type.
    */
  def decode(version: Int, bytes: Array[Byte]): Try[A]

  /** Decodes bytes written with the current [[version]]. */
  final override def decode(bytes: Array[Byte]): Try[A] = decode(version, bytes)

}
object EventCodec {
  /** Stable identifier of an event protocol codec. */
  type Tag = String
}
