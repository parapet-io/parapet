package io.parapet.core

trait InStream[F[_]] extends Stream[F] {
  def read: F[Array[Byte]]
}