package io.parapet.net.transport

/** A transport message body, preserved as the wire delivered it.
  *
  * `parts` is the full multipart sequence. For a single-part body, `parts` has exactly one element.
  *
  * Mutability note: `Array[Byte]` is mutable by the JVM. By convention, elements of `parts` are treated as effectively
  * immutable once the `Message` leaves the transport layer.
  */
final case class Message(parts: Vector[Array[Byte]]):
  def isEmpty: Boolean =
    parts.isEmpty

  def isSinglePart: Boolean =
    parts.sizeIs == 1

object Message:
  val empty: Message =
    Message(Vector.empty)

  def single(payload: Array[Byte]): Message =
    Message(Vector(payload))

  def of(parts: Array[Byte]*): Message =
    Message(parts.toVector)
