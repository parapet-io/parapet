package io.parapet.effect

import java.util.concurrent.CompletableFuture

final class Deferred[F[_], A] private (future: CompletableFuture[A])(using effect: Effect[F]):
  def get: F[A] =
    effect.blocking(future.get())

  def complete(value: A): F[Boolean] =
    effect.delay(future.complete(value))

object Deferred:
  def apply[F[_], A]()(using effect: Effect[F]): F[Deferred[F, A]] =
    effect.delay(new Deferred[F, A](CompletableFuture[A]()))
