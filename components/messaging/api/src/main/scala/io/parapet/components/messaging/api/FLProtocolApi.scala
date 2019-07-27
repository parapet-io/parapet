package io.parapet.components.messaging.api

import io.parapet.core.Event

object FLProtocolApi {

  sealed trait ControlEvent extends Event

  case class Connect(endpoint: String) extends ControlEvent

}
