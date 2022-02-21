package io.parapet.spark

import cats.effect.concurrent.Deferred
import io.parapet.core.Dsl.{DslF, WithDsl}
import io.parapet.spark.Api.{MapResult, TaskId}

import scala.collection.mutable

class Job[F[_]](tasks: Set[TaskId], done: Deferred[F, List[Row]]) extends WithDsl[F] {

  import dsl._

  private val completedTasks = mutable.Set.empty[TaskId]
  private val result = mutable.ListBuffer.empty[Row]

  def complete(mapResult: MapResult): DslF[F, Unit] = flow {
    if (completedTasks.add(mapResult.taskId)) {
      val (_, rows) = Codec.decodeDataframe(mapResult.data)
      result ++= rows
    }
    if (completedTasks == tasks) {
      suspend(done.complete(result.toList))
    } else {
      unit
    }

  }
}
