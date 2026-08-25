package io.parapet.journal

import io.parapet.core.Events.Registered
import io.parapet.{Event, ProcessRef}

import java.nio.charset.StandardCharsets.UTF_8
import scala.util.Try

private[journal] object RegisteredEventCodec extends EventCodec:
  val tag: String  = "parapet.registered"
  val version: Int = 1

  def encode(event: Event): Try[Array[Byte]] = Try {
    event match
      case Registered(child) => child.value.getBytes(UTF_8)
      case other             => throw new IllegalArgumentException(s"cannot encode $other")
  }

  def decode(encodedVersion: Int, bytes: Array[Byte]): Try[Event] = Try {
    if encodedVersion != version then
      throw new IllegalArgumentException(s"unsupported Registered schema version: $encodedVersion")
    Registered(ProcessRef[Event](new String(bytes, UTF_8)))
  }
