package io.parapet.net

/** Identifies the L4 transport protocol used by a [[NetworkAddress]]. */
enum TransportProtocol derives CanEqual:
  /** TCP — reliable, connection-oriented byte stream. */
  case Tcp
  /** UDP — datagram-oriented, unreliable, connectionless. */
  case Udp

  /** URI scheme prefix corresponding to this protocol. */
  def scheme: String =
    this match
      case Tcp => "tcp"
      case Udp => "udp"

/** A `protocol://host:port` triple identifying a network endpoint.
  *
  * Used by the [[Cluster]] view to advertise reachable peers and by transport processes
  * to bind/connect.
  */
final case class NetworkAddress(protocol: TransportProtocol, host: String, port: Int):
  /** Returns the canonical URI form, e.g. `tcp://localhost:5000`. */
  def uri: String =
    s"${protocol.scheme}://$host:$port"
