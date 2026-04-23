package io.parapet.net

enum TransportProtocol derives CanEqual:
  case Tcp
  case Udp

  def scheme: String =
    this match
      case Tcp => "tcp"
      case Udp => "udp"

final case class NetworkAddress(protocol: TransportProtocol, host: String, port: Int):
  def uri: String =
    s"${protocol.scheme}://$host:$port"
