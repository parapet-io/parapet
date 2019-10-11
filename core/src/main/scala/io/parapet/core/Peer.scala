package io.parapet.core

import io.parapet.core.Peer.PeerInfo

trait Peer[F[_]] {

  val info: PeerInfo

  def run: F[Unit]

  // closes all open connections
  def stop: F[Unit]

  def process: Process[F]

  /**
    * Connects this peer to another peer using provided network address.
    *
    * @param addr peer address
    * @return connection
    */
  // todo return type should be F[Either[Error, Connection[F]]]
  def connect(addr: String): F[Connection[F]]

}

object Peer {

  /**
    * Peer related information.
    *
    * @param addr      the peer network address
    * @param protocols list of supported protocols by this peer
    */
  case class PeerInfo(protocol: String,
                      host: String,
                      port: Int,
                      protocols: Set[String], ports: Ports)

}
