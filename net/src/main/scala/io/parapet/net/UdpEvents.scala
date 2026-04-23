package io.parapet.net

import io.parapet.Event

/** [[io.parapet.Event]] envelopes used to interact with a UDP datagram transport process. */
object UdpEvents:
  /** Outbound: publish `payload` to the configured peer set. */
  final case class Send(payload: Array[Byte]) extends Event

  /** Inbound: a datagram delivered by the underlying transport. */
  final case class Message(payload: Array[Byte]) extends Event
