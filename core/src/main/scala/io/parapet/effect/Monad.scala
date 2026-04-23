package io.parapet.effect

trait Monad[F[_]]:
  def pure[A](value: A): F[A]

  extension [A](fa: F[A])
    def flatMap[B](f: A => F[B]): F[B]

    def map[B](f: A => B): F[B] =
      flatMap(a => pure(f(a)))

object Monad:
  def apply[F[_]](using monad: Monad[F]): Monad[F] = monad

  def pure[F[_], A](value: A)(using monad: Monad[F]): F[A] =
    monad.pure(value)

  def sequence[F[_], A](values: Iterable[F[A]])(using monad: Monad[F]): F[List[A]] =
    values.foldRight(monad.pure(List.empty[A])) { (current, acc) =>
      current.flatMap(value => acc.map(value :: _))
    }

  def sequenceDiscard[F[_]](values: Iterable[F[Unit]])(using monad: Monad[F]): F[Unit] =
    values.foldLeft(monad.pure(())) { (acc, current) =>
      acc.flatMap(_ => current)
    }

  extension [F[_]: Monad, A](fa: F[A])
    def >>[B](fb: => F[B]): F[B] =
      fa.flatMap(_ => fb)

    def as[B](value: => B): F[B] =
      fa.map(_ => value)

    def void: F[Unit] =
      fa.as(())

    def through[B](fb: => F[B]): F[A] =
      fa.flatMap(a => fb.as(a))
