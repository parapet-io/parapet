package io.parapet.core

trait Connection[F[_]] {

  //  [protocol-name]/[version]
  def newSteam(protocolId: String): F[Stream[F]]

}
