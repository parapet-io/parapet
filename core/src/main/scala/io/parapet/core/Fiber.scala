package io.parapet.core

import io.parapet.core.Dsl.{DslF, WithDsl}
import io.parapet.effect.EffectFiber

trait Fiber[F[_], A]:
  def join: DslF[F, A]
  def cancel: DslF[F, Unit]

object Fiber:
  final class RuntimeFiber[F[_], A](fiber: EffectFiber[F, A]) extends Fiber[F, A] with WithDsl[F]:
    import dsl.*

    def join: DslF[F, A] =
      suspend(fiber.join)

    def cancel: DslF[F, Unit] =
      suspend(fiber.cancel)

  final class IdFiber[A](value: A) extends Fiber[[x] =>> x, A] with WithDsl[[x] =>> x]:
    import dsl.*

    def join: DslF[[x] =>> x, A] =
      pure(value)

    def cancel: DslF[[x] =>> x, Unit] =
      unit
