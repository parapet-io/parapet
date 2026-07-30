package io.parapet.core.journal

import io.parapet.Event

/** Encodes and decodes the events a process receives, so its delivery stream can be journaled and replayed.
  *
  * The codec is **process-owned and self-dispatching**: it covers every event type the process can receive, and any
  * type tag needed to tell them apart lives inside the produced bytes. The framework stores the payload opaque and, on
  * replay, uses the receiver's codec to turn it back into an [[Event]].
  *
  * This is the event-serialization counterpart to `Snapshotable` (state serialization); a recoverable process provides
  * both.
  */
trait EventCodec:

  /** Serializes an inbound event to bytes. */
  def encode(event: Event): Array[Byte]

  /** Reconstructs an event from bytes produced by [[encode]]. */
  def decode(bytes: Array[Byte]): Event
