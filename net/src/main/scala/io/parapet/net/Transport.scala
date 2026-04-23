package io.parapet.net

/** A single inbound frame received by a [[RequestResponseServer]].
  *
  * @param clientId opaque identifier the server uses to address replies back to the
  *                 originator.
  * @param payload  raw byte payload as delivered by the underlying transport.
  */
final case class ReceivedFrame(clientId: String, payload: Array[Byte])

/** Synchronous request/response transport client over an effect type `F`. */
trait RequestResponseClient[F[_]]:
  /** Sends `payload` and returns `Some(reply)` on success or `None` on timeout/loss. */
  def request(payload: Array[Byte]): F[Option[Array[Byte]]]

  /** Releases underlying transport resources. */
  def close: F[Unit]

/** Server side of a request/response transport. */
trait RequestResponseServer[F[_]]:
  /** Pulls the next inbound frame; `None` indicates the server has been closed. */
  def receive: F[Option[ReceivedFrame]]

  /** Sends `payload` back to the client identified by `clientId`. */
  def reply(clientId: String, payload: Array[Byte]): F[Unit]

  /** Releases underlying transport resources. */
  def close: F[Unit]

/** Datagram (UDP-style) transport used for fan-out and gossip.
  *
  * No correlation between send and receive; consumers process the buffered batch and
  * decide what (if anything) to do in response.
  */
trait DatagramTransport[F[_]]:
  /** Broadcasts `payload` to all configured peers. */
  def publish(payload: Array[Byte]): F[Unit]

  /** Drains up to `limit` buffered datagrams; returns earlier with whatever has arrived. */
  def receiveBatch(limit: Int): F[List[Array[Byte]]]

  /** Releases underlying transport resources. */
  def close: F[Unit]
