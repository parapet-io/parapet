package io.parapet.net

final case class ReceivedFrame(clientId: String, payload: Array[Byte])

trait RequestResponseClient[F[_]]:
  def request(payload: Array[Byte]): F[Option[Array[Byte]]]
  def close: F[Unit]

trait RequestResponseServer[F[_]]:
  def receive: F[Option[ReceivedFrame]]
  def reply(clientId: String, payload: Array[Byte]): F[Unit]
  def close: F[Unit]

trait DatagramTransport[F[_]]:
  def publish(payload: Array[Byte]): F[Unit]
  def receiveBatch(limit: Int): F[List[Array[Byte]]]
  def close: F[Unit]
