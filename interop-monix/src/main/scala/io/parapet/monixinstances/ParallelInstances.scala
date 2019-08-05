package io.parapet.monixinstances

import cats.effect.ContextShift
import io.parapet.core.Parallel
import monix.eval.Task
import cats.instances.list._
import cats.syntax.parallel._

trait ParallelInstances {
  implicit def scalazZioForParallel(implicit ctx: ContextShift[Task]): Parallel[Task] =
    (effects: Seq[Task[_]]) => effects.toList.parSequence_
}