package io.parapet.net.transport

trait DatagramTransport[F[_]]:
  def publish(message: Message): F[Either[TransportError, Unit]]
  def receiveBatch(limit: Int): F[ReceiveResult[Vector[Message]]]
  def close: F[Unit]
