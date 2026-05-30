package io.parapet.net.transport

trait DatagramTransport[F[_]]:
  def publish(datagram: Datagram): F[Either[TransportError, Unit]]
  def receiveBatch(limit: Int): F[ReceiveResult[Vector[Datagram]]]
  def close: F[Unit]
