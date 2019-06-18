package io.parapet.core

import cats.effect.concurrent.{MVar, Semaphore}
import cats.effect.syntax.bracket._
import cats.effect.Concurrent
import cats.syntax.flatMap._
import cats.syntax.functor._
import io.parapet.syntax.EffectOps
import io.parapet.syntax.boolean._

import scala.concurrent.duration.FiniteDuration

trait Lock[F[_]] {

  def acquire: F[Unit]

  def tryAcquire: F[Boolean]

  def tryAcquire(duration: FiniteDuration)(implicit ct: Concurrent[F]): F[Boolean] = {
    EffectOps.retryWithTimeout(tryAcquire.map(_.toOption(())), duration.fromNow).map(_.isDefined)
  }

  def release: F[Unit]

  def withPermit[A](body: => F[A])(implicit ct: Concurrent[F]): F[A] = {
    (acquire >> body).guaranteeCase(_ => release)
  }

  def withPermit[A](body: => F[A], duration: FiniteDuration)(implicit ct: Concurrent[F]): F[Option[A]] = {
    tryAcquire(duration).flatMap {
      case true => body >>= (a => release.map(_ => Option(a)))
      case false => ct.pure(Option.empty[A])
    }
  }

  def get: F[Long]

}

object Lock {
  def apply[F[_] : Concurrent]: F[Lock[F]] = Semaphore(1).map { s =>
    new Lock[F] {
      override def acquire: F[Unit] = s.acquire

      override def release: F[Unit] = s.release

      override def tryAcquire: F[Boolean] = s.tryAcquire

      override def get: F[Long] = s.available
    }
  }

  def mvar[F[_] : Concurrent]: F[Lock[F]] = MVar.of[F, Unit](()).map{ s =>
    new Lock[F] {
      override def acquire: F[Unit] = s.take

      override def release: F[Unit] = s.put(())

      override def tryAcquire: F[Boolean] = s.tryTake.map(_.isDefined)

      override def get: F[Long] = s.isEmpty.map(res => if(res) 1 else 0)
    }
  }

}