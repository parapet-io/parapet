package io.parapet.free

trait FunctionK[F[_], G[_]]:
  def apply[A](fa: F[A]): G[A]

type ~>[F[_], G[_]] = FunctionK[F, G]
