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
  * [[io.parapet.core.DslInterpreter]] need: lazy evaluation ([[delay]]), thread offload ([[blocking]]), error handling,
  * sleep, fork, and race.
  *
  * Concrete instances live in backend modules such as `parapet-cats-effect`, `parapet-pario`, or an
  * application-specific integration.
  */
trait Effect[F[_]] extends Monad[F]:
  /** Suspends a pure but lazy computation. */
  def delay[A](thunk: => A): F[A]

  /** Runs `thunk` on the runtime's blocking pool.
    *
    * Implementations may synchronously wait for the result unless they support true async suspension.
    */
  def blocking[A](thunk: => A): F[A]

  /** Suspends construction of an `F`-effect; lets users stitch effectful values lazily. */
  def suspend[A](thunk: => F[A]): F[A]

  /** Aborts the effect with `error`. */
  def raiseError[A](error: Throwable): F[A]

  /** Suspends for `duration`.
    *
    * Implementations may block a runtime thread unless they support true async suspension.
    */
  def sleep(duration: FiniteDuration): F[Unit]

  /** Forks `fa` as a [[EffectFiber]] running concurrently. */
  def start[A](fa: F[A]): F[EffectFiber[F, A]]

  /** Forks `fa` onto the runtime's blocking pool. Intended for operations like parapet's `offload`, where the
    * computation may block a thread and should not contend with ordinary async work.
    */
  def startBlocking[A](fa: F[A]): F[EffectFiber[F, A]]

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
