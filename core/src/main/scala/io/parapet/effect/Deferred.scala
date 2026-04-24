package io.parapet.effect

import java.util.concurrent.CompletableFuture

/** A "set once, await many" cell used to bridge synchronous code with the effect type `F`.
  *
  * Backed by a JDK [[CompletableFuture]] - readers wait via [[get]], writers fulfill via [[complete]]. Subsequent
  * completions are no-ops (the underlying future is set-once).
  */
final class Deferred[F[_], A] private (future: CompletableFuture[A])(using effect: Effect[F]):
  /** Suspends until [[complete]] runs, then returns the stored value. */
  def get: F[A] =
    effect.blocking(future.get())

  /** Tries to set the cell to `value`. Returns `true` on the first successful set. */
  def complete(value: A): F[Boolean] =
    effect.delay(future.complete(value))

/** [[Deferred]] factory. */
object Deferred:
  /** Allocates a fresh, empty [[Deferred]] in `F`. */
  def apply[F[_], A]()(using effect: Effect[F]): F[Deferred[F, A]] =
    effect.delay(new Deferred[F, A](CompletableFuture[A]()))
