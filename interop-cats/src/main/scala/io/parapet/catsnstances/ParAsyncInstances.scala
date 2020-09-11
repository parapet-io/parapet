package io.parapet.catsnstances

import cats.effect.IO
import io.parapet.core.ParAsync

trait ParAsyncInstances {
  implicit val parAsyncInstance: ParAsync[IO] = new ParAsync[IO] {
    override def runAsync[A](e: IO[A], cb: Either[Throwable, A] => IO[Unit]): IO[Unit] =
      e.runAsync(cb).toIO
  }
}
