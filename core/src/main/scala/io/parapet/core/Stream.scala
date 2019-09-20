package io.parapet.core

// Peer1 stream <-> Peer2 stream
//
trait Stream[F[_]] {
  def write(data: Array[Byte]): F[Unit]
  def read: F[Array[Byte]]
}
