package io.parapet.free

import io.parapet.effect.Monad

/** A minimal free monad over an algebra `F[_]`.
  *
  * Programs are built by composing values with [[Free.pure]] / [[Free.lift]] (and monadic operations) and then run by
  * folding into a target monad `G` via [[foldMap]].
  *
  * Used internally by [[io.parapet.core.Dsl]] to encode process programs as data so they can be inspected and
  * interpreted by the runtime.
  */
sealed trait Free[F[_], A]:
  /** Maps the result. */
  final def map[B](f: A => B): Free[F, B] =
    flatMap(a => Free.pure(f(a)))

  /** Sequences the next computation. */
  final def flatMap[B](f: A => Free[F, B]): Free[F, B] =
    Free.FlatMapped(this, f)

  /** Interprets this program by translating each `F` operation through `fk` into the target monad `G`. The
    * implementation is trampolined to avoid stack overflows on deeply nested binds.
    */
  final def foldMap[G[_]](fk: FunctionK[F, G])(using G: Monad[G]): G[A] =
    Free.foldMap(this, fk)

/** Constructors and the trampolined interpreter for [[Free]]. */
object Free:
  /** Lifts a value with no further effects. */
  final case class Pure[F[_], A](value: A) extends Free[F, A]

  /** Embeds a single `F` operation. */
  final case class Suspend[F[_], A](value: F[A]) extends Free[F, A]

  /** Sequencing constructor used by [[Free.flatMap]]. */
  final case class FlatMapped[F[_], A, B](source: Free[F, A], bind: A => Free[F, B]) extends Free[F, B]

  /** Lifts a pure value into `Free`. */
  def pure[F[_], A](value: A): Free[F, A] =
    Pure(value)

  /** Lifts a single `F`-effect into `Free`. */
  def lift[F[_], A](value: F[A]): Free[F, A] =
    Suspend(value)

  /** Lifts an `F`-effect into a coproduct algebra `G` via an [[Inject]] witness - used to mix multiple algebras (e.g.
    * parapet DSL + a domain-specific algebra) into a single program.
    */
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
