package io.parapet.core

/** Type class providing concurrent execution for the effect type `F`.
  *
  * The runtime relies on a [[Parallel]] instance to spin up [[Scheduler]] worker fibers in
  * parallel and to fan out child-process shutdowns. Concrete instances are supplied by
  * effect-specific subclasses of [[io.parapet.ParApp]] (e.g.
  * [[io.parapet.effect.ParIO.parallel]]).
  */
trait Parallel[F[_]]:
  /** Executes `effects` concurrently and returns once all have completed. */
  def par(effects: Seq[F[Unit]]): F[Unit]

/** Summoner for [[Parallel]] type-class instances. */
object Parallel:
  /** Conventional "implicitly[Parallel[F]]" accessor. */
  def apply[F[_]](using parallel: Parallel[F]): Parallel[F] = parallel
