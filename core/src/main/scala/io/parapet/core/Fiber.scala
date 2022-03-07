package io.parapet.core

import cats.Id
import io.parapet.core.Dsl.{DslF, WithDsl}

trait Fiber[F[_], A] {

  def join: DslF[F, A]

  def cancel: DslF[F, Unit]
}

object Fiber {
  class CatsEffect[F[_], A](f: cats.effect.Fiber[F, A]) extends Fiber[F, A] with WithDsl[F] {

    import dsl._

    override def join: DslF[F, A] = suspend(f.join)

    override def cancel: DslF[F, Unit] = suspend(f.cancel)
  }

  class IdFiber[A](a: Id[A]) extends Fiber[Id, A] with WithDsl[Id] {

    import dsl._

    override def join: DslF[Id, A] = pure(a)

    override def cancel: DslF[Id, Unit] = unit
  }
}
