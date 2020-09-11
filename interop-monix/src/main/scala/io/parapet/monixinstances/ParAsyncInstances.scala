package io.parapet.monixinstances

import io.parapet.core.ParAsync
import monix.eval.Task

trait ParAsyncInstances {
  implicit val parAsyncInstance: ParAsync[Task] = new ParAsync[Task] {
    override def runAsync[A](e: Task[A], cb: Either[Throwable, A] => Task[Unit]): Task[Unit] =
      e.map(_ => ())
  }
}
