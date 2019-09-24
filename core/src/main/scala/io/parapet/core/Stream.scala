package io.parapet.core


// todo: consider to split into in/out streams
trait Stream[F[_]] {
  // todo add id
  def write(data: Array[Byte]): F[Unit]

  def read: F[Array[Byte]]
}
