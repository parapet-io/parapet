package io.parapet.core

import cats.Monad
import cats.effect.{Concurrent, Timer}
import cats.effect.concurrent.Semaphore
import cats.syntax.flatMap._
import cats.syntax.functor._

import scala.concurrent.duration.FiniteDuration

trait Lock[F[_]] {

  def acquire: F[Unit]

  def tryAcquire(time: FiniteDuration): F[Boolean]

  def release: F[Unit]

  def safe[A](body: => F[A])(implicit M: Monad[F]): F[A] = {
    acquire >> body >>= (a => release.map(_ => a))
  }

  def safe[A](body: => F[A], time: FiniteDuration)(implicit M: Monad[F]): F[Option[A]] = {
    tryAcquire(time).flatMap {
      case true => body >>= (a => release.map(_ => Option(a)))
      case false => M.pure(Option.empty[A])
    }
  }

}

object Lock {
  def apply[F[_] : Concurrent : Timer]: F[Lock[F]] = Semaphore(1).map { s =>
    new Lock[F] {
      override def acquire: F[Unit] = s.acquire

      override def release: F[Unit] = s.release

      override def tryAcquire(time: FiniteDuration): F[Boolean] = {
        Concurrent[F].race(s.acquire, Timer[F].sleep(time)).flatMap {
          case Left(_) => Concurrent[F].pure(true)
          case Right(_) => Concurrent[F].pure(false)
        }
      }
    }
  }
}