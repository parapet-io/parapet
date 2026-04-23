package io.parapet.effect

import scala.concurrent.duration.FiniteDuration

trait EffectFiber[F[_], A]:
  def join: F[A]
  def cancel: F[Unit]

trait Effect[F[_]] extends Monad[F]:
  def delay[A](thunk: => A): F[A]
  def blocking[A](thunk: => A): F[A]
  def suspend[A](thunk: => F[A]): F[A]
  def raiseError[A](error: Throwable): F[A]
  def sleep(duration: FiniteDuration): F[Unit]
  def start[A](fa: F[A]): F[EffectFiber[F, A]]
  def race[A, B](left: F[A], right: F[B]): F[Either[A, B]]
  def guarantee[A](fa: F[A])(finalizer: F[Unit]): F[A]

  extension [A](fa: F[A])
    def handleErrorWith(f: Throwable => F[A]): F[A]

object Effect:
  def apply[F[_]](using effect: Effect[F]): Effect[F] = effect

  extension [F[_]: Effect, A](fa: F[A])
    def handleErrorWith(f: Throwable => F[A]): F[A] =
      summon[Effect[F]].handleErrorWith(fa)(f)

    def guarantee(finalizer: F[Unit]): F[A] =
      summon[Effect[F]].guarantee(fa)(finalizer)

    def attempt: F[Either[Throwable, A]] =
      fa.map(Right(_)).handleErrorWith(error => summon[Effect[F]].pure(Left(error)))
