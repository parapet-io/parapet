package io.parapet.net.transport

trait ClientTransport[F[_]]:
  def request(message: Message): F[Either[TransportError, Message]]
  def close: F[Unit]
