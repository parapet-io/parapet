package io.parapet.core

import io.parapet.effect.Effect

import java.util.concurrent.Semaphore

trait Lock[F[_]]:
  def acquire: F[Unit]
  def tryAcquire: F[Boolean]
  def release: F[Unit]
  def isAcquired: F[Boolean]

  def withPermit[A](body: => F[A])(using effect: Effect[F]): F[A] =
    effect.guarantee(acquire.flatMap(_ => body))(release)

object Lock:
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
