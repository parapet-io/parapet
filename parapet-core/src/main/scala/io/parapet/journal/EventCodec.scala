package io.parapet.journal

import io.parapet.Event

import scala.util.Try

/** Encodes and decodes events.
  */
trait EventCodec {
  import EventCodec._

  /** Unique codec id. */
  def tag: Tag

  /** Schema version. */
  def version: Int

  /** Serializes an event to bytes. Always writes the current [[version]]. */
  def encode(event: Event): Try[Array[Byte]]

  /** Reconstructs an event from bytes produced by [[encode]] at `version`. An implementation must decode every version
    * it has ever written, mapping each to the current in-memory event.
    */
  def decode(version: Int, bytes: Array[Byte]): Try[Event]

  /** Convenience: decodes bytes produced at the current [[version]].
    */
  def decode(bytes: Array[Byte]): Try[Event] = decode(version, bytes)
}
object EventCodec {
  type Tag = String
}
