package io.parapet.effect

import io.parapet.effect.Monad.*

/** Retries a failing effect with a [[Backoff]] schedule. */
object Retry:

  /** Runs `fa`, retrying on failure up to `maxRetries` times with `backoff` between attempts; the last error is
    * propagated once retries are exhausted.
    *
    * @param maxRetries
    *   retries after the initial attempt; must be `>= 0`.
    * @param onRetry
    *   hook invoked as `(retry, error)` before each retry.
    */
  def apply[F[_], A](
      maxRetries: Int,
      backoff: Backoff,
      onRetry: (Int, Throwable) => Unit = (_: Int, _: Throwable) => ()
  )(fa: => F[A])(using effect: Effect[F]): F[A] =
    require(maxRetries >= 0, "maxRetries must be >= 0")

    def loop(retries: Int): F[A] =
      fa.handleErrorWith { error =>
        if retries >= maxRetries then effect.raiseError(error)
        else
          val next = retries + 1
          effect.delay(onRetry(next, error)) >> effect.sleep(backoff.duration(next)) >> loop(next)
      }

    loop(0)
