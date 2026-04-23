package io.parapet.core

import io.parapet.effect.Effect

import java.util.concurrent.Semaphore

/** A binary mutex over the effect type `F`.
  *
  * Backed by a single-permit `java.util.concurrent.Semaphore`. The acquiring fiber may
  * block on the underlying thread when no permit is available — implementations dispatch
  * the wait through [[Effect.blocking]] so it does not stall a scheduler worker.
  */
trait Lock[F[_]]:
  /** Suspends until the permit is acquired. */
  def acquire: F[Unit]

  /** Non-blocking attempt to acquire the permit. Returns `true` on success. */
  def tryAcquire: F[Boolean]

  /** Releases the permit. Calling without a prior successful acquire is undefined. */
  def release: F[Unit]

  /** Returns `true` while the lock is held. */
  def isAcquired: F[Boolean]

  /** Runs `body` while holding the lock and releases it afterwards regardless of success
    * or failure.
    */
  def withPermit[A](body: => F[A])(using effect: Effect[F]): F[A] =
    effect.guarantee(acquire.flatMap(_ => body))(release)

/** [[Lock]] factory. */
object Lock:
  /** Allocates a fresh `Lock` in `F`. */
  def apply[F[_]]()(using effect: Effect[F]): F[Lock[F]] =
    effect.delay {
      val semaphore = new Semaphore(1)
      new Lock[F]:
        def acquire: F[Unit] =
          effect.blocking {
            semaphore.acquire()
            ()
          }

        def tryAcquire: F[Boolean] =
          effect.delay(semaphore.tryAcquire())

        def release: F[Unit] =
          effect.delay {
            semaphore.release()
            ()
          }

        def isAcquired: F[Boolean] =
          effect.delay(semaphore.availablePermits() == 0)
    }
