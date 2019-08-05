package io.parapet.zioinstances

import cats.effect.ContextShift
import cats.instances.list._
import cats.syntax.parallel._
import io.parapet.core.Parallel
import scalaz.zio.Task
import scalaz.zio.interop.catz._

trait ParallelInstances {
  implicit def scalazZioForParallel(implicit ctx: ContextShift[Task]): Parallel[Task] =
    (effects: Seq[Task[_]]) => effects.toList.parSequence_
}
