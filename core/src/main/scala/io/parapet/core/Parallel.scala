package io.parapet.core

/** Type class providing concurrent execution for the effect type `F`.
  *
  * The runtime relies on a [[Parallel]] instance for finite parallel batches such as child-process shutdown. Long-lived
  * scheduler worker loops use the internal [[SchedulerRuntime]] capability instead.
  */
trait Parallel[F[_]]:
  /** Executes `effects` concurrently and returns once all have completed. */
  def par(effects: Seq[F[Unit]]): F[Unit]

/** Summoner for [[Parallel]] type-class instances. */
object Parallel:
  /** Conventional "implicitly[Parallel[F]]" accessor. */
  def apply[F[_]](using parallel: Parallel[F]): Parallel[F] = parallel
