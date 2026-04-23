package io.parapet.free

import io.parapet.effect.Monad

sealed trait Free[F[_], A]:
  final def map[B](f: A => B): Free[F, B] =
    flatMap(a => Free.pure(f(a)))

  final def flatMap[B](f: A => Free[F, B]): Free[F, B] =
    Free.FlatMapped(this, f)

  final def foldMap[G[_]](fk: FunctionK[F, G])(using G: Monad[G]): G[A] =
    Free.foldMap(this, fk)

object Free:
  final case class Pure[F[_], A](value: A) extends Free[F, A]
  final case class Suspend[F[_], A](value: F[A]) extends Free[F, A]
  final case class FlatMapped[F[_], A, B](source: Free[F, A], bind: A => Free[F, B]) extends Free[F, B]

  def pure[F[_], A](value: A): Free[F, A] =
    Pure(value)

  def lift[F[_], A](value: F[A]): Free[F, A] =
    Suspend(value)

  def inject[F[_], G[_], A](value: F[A])(using inject: Inject[F, G]): Free[G, A] =
    Suspend(inject.inject(value))

  private def foldMap[F[_], G[_], A](free: Free[F, A], fk: FunctionK[F, G])(using G: Monad[G]): G[A] =
    free match
      case Pure(value) =>
        G.pure(value)
      case Suspend(value) =>
        fk(value)
      case FlatMapped(source, bind) =>
        source match
          case Pure(value) =>
            foldMap(bind(value), fk)
          case Suspend(value) =>
            fk(value).flatMap(a => foldMap(bind(a), fk))
          case FlatMapped(source0, bind0) =>
            foldMap(source0.flatMap(x => bind0(x).flatMap(bind)), fk)
