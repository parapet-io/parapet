package io.parapet.net

import io.parapet.net.Address.Transport

case class Address(transport: Transport, host: String, port: Int) {
  val value = s"${transport.name}://$host:$port"
  val simple = s"$host:$port"
}

object Address {

  sealed trait Transport {
    val name: String

    final override def toString: String = name
  }

  object Transport {

    /**
      * The TCP transport.
      */
    case object Tcp extends Transport {
      override val name: String = "tcp"
    }

    /**
      * The inter-thread transport.
      */
    case object Inproc extends Transport {
      override val name: String = "inproc"
    }

    def apply(str: String): Transport = {
      str.toLowerCase match {
        case Tcp.name => Tcp
        case Inproc.name => Inproc
        case _ => throw new IllegalArgumentException(s"unsupported protocol: $str")
      }
    }
  }

  private val fullAddressPattern = "([a-z]+)://(\\S+):(\\d{2,})".r
  private val addressPattern = "(\\S+):(\\d{2,})".r

  def apply(address: String): Address = {
    address match {
      case fullAddressPattern(transport, host, port) => new Address(Transport(transport), host, port.toInt)
      case _ => throw new IllegalArgumentException(s"bad address: $address")
    }
  }

  def tcp(host: String, port: Int): Address = new Address(Transport.Tcp, host, port)

  def tcp(address: String): Address = {
    address.toLowerCase match {
      case addressPattern(host, port) => tcp(host, port.toInt)
      case _ => throw new IllegalArgumentException(s"bad address: $address")
    }
  }
}
