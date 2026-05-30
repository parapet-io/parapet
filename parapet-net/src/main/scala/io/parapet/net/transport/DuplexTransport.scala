package io.parapet.net.transport

trait DuplexTransport[F[_]]:
  def send(message: Message): F[Either[TransportError, Unit]]
  def receive: F[ReceiveResult[Message]]
  def close: F[Unit]
