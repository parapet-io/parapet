package io.parapet.net

enum TransportProtocol derives CanEqual:

  case Tcp
  case Udp

  /** URI scheme prefix corresponding to this protocol. */
  def scheme: String =
    this match
      case Tcp => "tcp"
      case Udp => "udp"

/** A `protocol://host:port` triple identifying a network endpoint. */
final case class Endpoint(protocol: TransportProtocol, host: String, port: Int):
  /** Returns the canonical URI form, e.g. `tcp://localhost:5000`. */
  def uri: String =
    s"${protocol.scheme}://$host:$port"
