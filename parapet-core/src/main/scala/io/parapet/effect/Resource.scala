package io.parapet.effect

import io.parapet.effect.Effect.*
import io.parapet.effect.Monad.*

/** Minimal acquire/use/release resource for effect types.
  */
final class Resource[F[_], A] private (val allocated: F[(A, F[Unit])]):
  def use[B](f: A => F[B])(using effect: Effect[F]): F[B] =
    effect.suspend {
      allocated.flatMap { case (value, release) =>
        f(value).guarantee(release)
      }
    }

object Resource:
  def make[F[_]: Effect, A](acquire: F[A])(release: A => F[Unit]): Resource[F, A] =
    val effect = Effect[F]
    Resource(acquire.map(value => (value, release(value))))
