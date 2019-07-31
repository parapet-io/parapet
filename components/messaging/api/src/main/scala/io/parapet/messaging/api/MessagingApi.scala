package io.parapet.messaging.api

import io.parapet.core.Event

object MessagingApi {

  case class Request(data: Event) extends Event

  sealed trait Response extends Event

  case class Success(event: Event) extends Response

  case class Failure(err: String, code: Int) extends Response

  @deprecated
  case class WithId(id: String, event: Event) extends Event

  implicit class EventOps(event: Event) {
    @deprecated
    def withId(id: String): Event = WithId(id, event)
  }

}
