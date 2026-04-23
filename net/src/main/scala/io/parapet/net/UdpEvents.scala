package io.parapet.net

import io.parapet.Event

object UdpEvents:
  final case class Send(payload: Array[Byte]) extends Event
  final case class Message(payload: Array[Byte]) extends Event
