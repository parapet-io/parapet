package io.parapet.effect

/** Minimal monad type-class used internally by parapet to keep the runtime independent of any particular effect
  * library.
  *
  * Implementations need only define [[pure]] and `flatMap`; `map` derives by default.
  */
trait Monad[F[_]]:
  /** Lifts a pure value into the monad. */
  def pure[A](value: A): F[A]

  extension [A](fa: F[A])
    /** Sequences `f` after `fa`, threading the result. */
    def flatMap[B](f: A => F[B]): F[B]

    /** Maps over the result. */
    def map[B](f: A => B): F[B] =
      flatMap(a => pure(f(a)))

/** Convenience helpers and extension methods for [[Monad]]. */
object Monad:
  /** Summons a [[Monad]] instance for `F`. */
  def apply[F[_]](using monad: Monad[F]): Monad[F] = monad

  /** Lifts `value` into `F`. */
  def pure[F[_], A](value: A)(using monad: Monad[F]): F[A] =
    monad.pure(value)

  /** Sequentially evaluates each effect and collects the results in order. */
  def sequence[F[_], A](values: Iterable[F[A]])(using monad: Monad[F]): F[List[A]] =
    values.foldRight(monad.pure(List.empty[A])) { (current, acc) =>
      current.flatMap(value => acc.map(value :: _))
    }

  /** Like [[sequence]] but discards the unit results. */
  def sequenceDiscard[F[_]](values: Iterable[F[Unit]])(using monad: Monad[F]): F[Unit] =
    values.foldLeft(monad.pure(())) { (acc, current) =>
      acc.flatMap(_ => current)
    }

  extension [F[_]: Monad, A](fa: F[A])
    /** Sequences `fa` then `fb`, discarding the first result. */
    def >>[B](fb: => F[B]): F[B] =
      fa.flatMap(_ => fb)

    /** Replaces `fa`'s result with the lazily-computed `value`. */
    def as[B](value: => B): F[B] =
      fa.map(_ => value)

    /** Discards `fa`'s result. */
    def void: F[Unit] =
      fa.as(())

    /** Runs `fa`, then `fb`, returning `fa`'s result. */
    def through[B](fb: => F[B]): F[A] =
      fa.flatMap(a => fb.as(a))
