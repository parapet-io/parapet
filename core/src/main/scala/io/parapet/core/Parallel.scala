package io.parapet.core

trait Parallel[F[_]] {

  /**
    * Runs the given effects in parallel and returns an effect with no value.
    *
    * @param effects the effects that should be processes in parallel.
    * @return Unit
    */
  def par(effects: Seq[F[_]]): F[Unit]
}

object Parallel {
  def apply[F[_] : Parallel]: Parallel[F] = implicitly[Parallel[F]]
}