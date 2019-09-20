package io.parapet.core

trait Peer[F[_]] {

  val addr: String

  def run: F[Unit]

  //
  // Command: {Connect=1}
  // ...
  def connect(addr0: String): F[Connection[F]]

}

object Peer {
  //  protocol

  case class PeerInfo(addr: String)

}
