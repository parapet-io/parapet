package io.parapet.core

trait InStream[F[_]] {
  def read: F[Array[Byte]]
}
