package io.parapet.core

trait Connection[F[_]] {

  // closes open stream sources
  def close: F[Unit]

  //  [protocol-name]/[version]
  def newSteam(protocolId: String): F[StreamSource[F]]

}