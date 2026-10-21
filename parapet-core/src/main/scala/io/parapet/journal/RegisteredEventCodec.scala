package io.parapet.journal

import io.parapet.Event.Registered
import io.parapet.EventCodec.Schema
import io.parapet.{Event, EventCodec, ProcessRef}
import io.parapet.Codec
import io.parapet.Codec.given

private[parapet] object RegisteredEventCodec extends EventCodec.Simple[Registered]:
  val schema: Schema.Simple    = Schema.Simple("", "parapet.registered")
  val version: Int             = 1
  val codec: Codec[Registered] = Codec.derived

  override def codec(version: Int): Option[Codec[Registered]] = version match {
    case 1 => Option(codec)
    case _ => Option.empty
  }
