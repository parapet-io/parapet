package io.parapet.spark

import cats.effect.IO
import cats.effect.concurrent.Deferred
import io.parapet.spark.Api.Task
import io.parapet.spark.Api.TaskResult

class Job(val id: String,
          tasks: Seq[Task],
          val done: Deferred[IO, Unit]) {

  private var _completed: Int = 0

  def complete(res: TaskResult): IO[Unit] = IO.suspend {
    _completed = _completed + 1
    if (_completed == tasks.size) {
      done.complete(())
    } else {
      IO.unit
    }
  }

}
