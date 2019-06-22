package io.parapet.core

import cats.effect.Concurrent
import cats.effect.concurrent.{MVar, Semaphore}
import cats.effect.syntax.bracket._
import cats.syntax.flatMap._
import cats.syntax.functor._

trait Lock[F[_]] {

  def acquire: F[Unit]

  def tryAcquire: F[Boolean]

  def release: F[Unit]

  def withPermit[A](body: => F[A])(implicit ct: Concurrent[F]): F[A] = {
    (acquire >> body).guaranteeCase(_ => release)
  }

  def isAcquired: F[Boolean]

}

object Lock {
  def apply[F[_] : Concurrent]: F[Lock[F]] = Semaphore(1).map { s =>
    new Lock[F] {
      override def acquire: F[Unit] = s.acquire

      override def release: F[Unit] = s.release

      override def tryAcquire: F[Boolean] = s.tryAcquire

      override def isAcquired: F[Boolean] = s.available.map(_ == 1)
    }
  }

  def mvar[F[_] : Concurrent]: F[Lock[F]] = MVar.of[F, Unit](()).map{ s =>
    new Lock[F] {
      override def acquire: F[Unit] = s.take

      override def release: F[Unit] = s.put(())

      override def tryAcquire: F[Boolean] = s.tryTake.map(_.isDefined)

      override def isAcquired: F[Boolean] = s.isEmpty
    }
  }

}