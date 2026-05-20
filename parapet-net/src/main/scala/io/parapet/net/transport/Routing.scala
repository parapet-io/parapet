package io.parapet.net.transport

opaque type RoutingId = String

object RoutingId:
  def apply(value: String): RoutingId =
    value

  extension (id: RoutingId) def value: String = id

final case class RoutedMessage(routingId: RoutingId, message: Message)
