package io.parapet.core

trait OutStream[F[_]] {
  def write(data: Array[Byte]): F[Unit]
}
