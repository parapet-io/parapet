package io.parapet.core

trait ParAsync[F[_]] {
  def runAsync[A](e: F[A], cb: Either[Throwable, A] => F[Unit]): F[Unit]
}

object ParAsync {
  def apply[F[_] : ParAsync]: ParAsync[F] = implicitly[ParAsync[F]]

}
