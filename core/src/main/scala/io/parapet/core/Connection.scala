package io.parapet.core

trait Connection[F[_]] {

  // closes open streams
  def close: F[Unit]

  //  [protocol-name]/[version]
  def newSteam(protocolId: String): F[Stream[F]]

  //
  def add(protocolId: String, stream: Stream[F]): Boolean

}
