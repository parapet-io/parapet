package io.parapet.free

/** A natural transformation: a polymorphic function from `F[A]` to `G[A]` for every `A`.
  *
  * The shape parapet uses to translate algebra operations during [[Free.foldMap]].
  */
trait FunctionK[F[_], G[_]]:
  /** Translates a single `F`-shaped value into the corresponding `G`-shaped value. */
  def apply[A](fa: F[A]): G[A]

/** Infix alias for [[FunctionK]]. */
type ~>[F[_], G[_]] = FunctionK[F, G]
