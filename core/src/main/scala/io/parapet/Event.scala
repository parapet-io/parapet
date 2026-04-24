package io.parapet

/** Marker trait for any value that can flow between [[io.parapet.core.Process]] instances.
  *
  * Every message exchanged through the parapet runtime - system signals, user commands, dead-letter notifications,
  * network frames - must extend `Event`. Events are typically implemented as immutable case classes so that they can be
  * safely shared between fibers without defensive copying.
  *
  * Two general-purpose payload events are provided in the companion object. Domain code is expected to define its own
  * event hierarchies (often as a sealed family) for type-safe pattern matching inside a process's
  * [[io.parapet.core.Process.handle]].
  */
trait Event

/** Built-in [[Event]] implementations for use cases where defining a dedicated type is unnecessary, such as
  * transport-layer payloads or quick prototyping.
  */
object Event:
  /** An [[Event]] carrying a raw byte payload.
    *
    * Useful for transport processes that operate on opaque buffers and delegate decoding to upstream handlers.
    *
    * @param data
    *   the raw bytes; ownership is transferred to the event and must not be mutated after construction.
    */
  final case class ByteEvent(data: Array[Byte]) extends Event:
    override def toString: String = new String(data)

  /** An [[Event]] carrying a single string payload.
    *
    * @param value
    *   the payload text.
    */
  final case class StringEvent(value: String) extends Event:
    override def toString: String = value
