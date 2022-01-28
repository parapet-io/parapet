package io.parapet.spark

import cats.effect.IO
import cats.effect.concurrent.Deferred
import io.parapet.spark.Api.Task
import io.parapet.spark.Api.TaskResult

import scala.collection.mutable.ListBuffer

class Job(val id: String,
          tasks: Seq[Task],
          val done: Deferred[IO, Unit]) {

  private var _completed: Int = 0
  private val _results = new ListBuffer[Row]

  def results: Seq[Row] = _results.toSeq

  def complete(res: TaskResult): IO[Unit] = IO.suspend {
    res match {
      case Api.MapResult(_, _, data) => {
        val (_, rows) = Codec.decodeDataframe(data)
        results ++= rows
      }
    }
    _completed = _completed + 1
    if (_completed == tasks.size) {
      done.complete(())
    } else {
      IO.unit
    }
  }

}
