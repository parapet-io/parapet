package io.parapet.core

import io.parapet.core.Dsl.{DslF, WithDsl}
import io.parapet.effect.EffectFiber

/** Handle to a forked, concurrently-running [[Dsl]] computation.
  *
  * Returned from [[Dsl.FlowOps.fork]]. Use [[join]] to await its result, or [[cancel]] to
  * stop it before completion.
  */
trait Fiber[F[_], A]:
  /** Suspends until the fiber completes and returns its result. */
  def join: DslF[F, A]

  /** Requests cancellation. Idempotent. */
  def cancel: DslF[F, Unit]

/** [[Fiber]] implementations. */
object Fiber:
  /** A real concurrent [[Fiber]] backed by an [[EffectFiber]] from the underlying effect
    * runtime.
    */
  final class RuntimeFiber[F[_], A](fiber: EffectFiber[F, A]) extends Fiber[F, A] with WithDsl[F]:
    import dsl.*

    def join: DslF[F, A] =
      suspend(fiber.join)

    def cancel: DslF[F, Unit] =
      suspend(fiber.cancel)

  /** A trivial fiber that wraps an already-known value; used by the `Id` interpreter where
    * fork is degenerate.
    */
  final class IdFiber[A](value: A) extends Fiber[[x] =>> x, A] with WithDsl[[x] =>> x]:
    import dsl.*

    def join: DslF[[x] =>> x, A] =
      pure(value)

    def cancel: DslF[[x] =>> x, Unit] =
      unit
