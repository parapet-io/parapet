package io.parapet.core

trait Parallel[F[_]]:
  def par(effects: Seq[F[Unit]]): F[Unit]

object Parallel:
  def apply[F[_]](using parallel: Parallel[F]): Parallel[F] = parallel
