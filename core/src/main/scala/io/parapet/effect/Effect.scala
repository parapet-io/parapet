package io.parapet.effect

import scala.concurrent.duration.FiniteDuration

/** Handle to a forked computation in the effect type `F`.
  *
  * Used by [[io.parapet.core.Fiber]] to wrap effect-level fibers in a DSL-friendly form.
  */
trait EffectFiber[F[_], A]:
  /** Suspends until the fiber finishes and returns its result. */
  def join: F[A]

  /** Requests cancellation. Idempotent. */
  def cancel: F[Unit]

/** Capability bundle the parapet runtime requires of any effect type `F`.
  *
  * Extends [[Monad]] with the additional primitives the [[io.parapet.core.Scheduler]] and
  * [[io.parapet.core.DslInterpreter]] need: lazy evaluation ([[delay]]), thread offload
  * ([[blocking]]), error handling, sleep, fork, and race.
  *
  * Concrete instances live alongside their effect type — for example
  * [[io.parapet.effect.ParIO.effect]] and [[io.parapet.effect.ParIO.parallel]] for parapet's
  * built-in [[ParIO]].
  */
trait Effect[F[_]] extends Monad[F]:
  /** Suspends a pure but lazy computation. */
  def delay[A](thunk: => A): F[A]

  /** Suspends a blocking computation, dispatched off the scheduler's worker threads. */
  def blocking[A](thunk: => A): F[A]

  /** Suspends construction of an `F`-effect; lets users stitch effectful values lazily. */
  def suspend[A](thunk: => F[A]): F[A]

  /** Aborts the effect with `error`. */
  def raiseError[A](error: Throwable): F[A]

  /** Non-blocking sleep for `duration`. */
  def sleep(duration: FiniteDuration): F[Unit]

  /** Forks `fa` as a [[EffectFiber]] running concurrently. */
  def start[A](fa: F[A]): F[EffectFiber[F, A]]

  /** Races `left` against `right`; the loser is cancelled. */
  def race[A, B](left: F[A], right: F[B]): F[Either[A, B]]

  /** Runs `fa` and runs `finalizer` afterward regardless of success/failure. */
  def guarantee[A](fa: F[A])(finalizer: F[Unit]): F[A]

  extension [A](fa: F[A])
    /** Recovers from an exception via `f`. */
    def handleErrorWith(f: Throwable => F[A]): F[A]

/** Convenience accessors and extension methods for [[Effect]]. */
object Effect:
  /** Summons an [[Effect]] instance for `F`. */
  def apply[F[_]](using effect: Effect[F]): Effect[F] = effect

  extension [F[_]: Effect, A](fa: F[A])
    /** Recovers from an exception via `f`. */
    def handleErrorWith(f: Throwable => F[A]): F[A] =
      summon[Effect[F]].handleErrorWith(fa)(f)

    /** Runs `fa` and `finalizer` afterward regardless of outcome. */
    def guarantee(finalizer: F[Unit]): F[A] =
      summon[Effect[F]].guarantee(fa)(finalizer)

    /** Materializes a possible failure as an `Either`. */
    def attempt: F[Either[Throwable, A]] =
      fa.map(Right(_)).handleErrorWith(error => summon[Effect[F]].pure(Left(error)))
