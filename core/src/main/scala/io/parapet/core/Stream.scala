package io.parapet.core

trait Stream[F[_]] {
  def close: F[Unit]
}
