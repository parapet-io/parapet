package io.parapet.journal

import io.parapet.Event.Registered
import io.parapet.{Event, EventCodec, ProcessRef}

import java.nio.charset.StandardCharsets.UTF_8
import scala.util.Try

private[parapet] object RegisteredEventCodec extends EventCodec[Registered]:
  val tag: String  = "parapet.registered"
  val version: Int = 1

  def encode(event: Registered): Try[Array[Byte]] = Try(event.child.value.getBytes(UTF_8))

  def decode(encodedVersion: Int, bytes: Array[Byte]): Try[Registered] = Try {
    if encodedVersion != version then
      throw new IllegalArgumentException(s"unsupported Registered schema version: $encodedVersion")
    Registered(ProcessRef[Event](new String(bytes, UTF_8)))
  }
