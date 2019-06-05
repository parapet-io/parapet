package io.parapet.core

trait Parallel[F[_]] {
  // runs given effects in parallel and returns a single effect
  def par(effects: Seq[F[_]]): F[Unit]
}

object Parallel {
  def apply[F[_] : Parallel]: Parallel[F] = implicitly[Parallel[F]]
}