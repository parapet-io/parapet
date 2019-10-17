package io.parapet.core

trait OutStream[F[_]] extends Stream [F] {

  def write(data: Array[Byte]): F[Unit]
}
