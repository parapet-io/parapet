package io.parapet.core.journal

import io.parapet.Event

import scala.util.Try

/** Encodes and decodes events.
  */
trait EventCodec:

  /** Serializes an event to bytes. */
  def encode(event: Event): Try[Array[Byte]]

  /** Reconstructs an event from bytes produced by [[encode]]. */
  def decode(bytes: Array[Byte]): Try[Event]
