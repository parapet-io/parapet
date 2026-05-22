package io.parapet.net.transport

trait ServerTransport[F[_]]:
  def receive: F[ReceiveResult[RoutedMessage]]
  def reply(routingId: RoutingId, message: Message): F[Either[TransportError, Unit]]
  def close: F[Unit]
