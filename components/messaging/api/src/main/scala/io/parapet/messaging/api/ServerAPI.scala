package io.parapet.messaging.api

import io.parapet.core.Event

object ServerAPI {

  case class Envelope(requestId: String, data: Event) extends Event

}
