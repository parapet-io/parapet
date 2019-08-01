package io.parapet.zioinstances

import cats.effect.ContextShift
import io.parapet.core.Parallel
import scalaz.zio.Task
import scalaz.zio.interop.catz.implicits._
import scalaz.zio.interop.catz._
import cats.instances.list._
import cats.syntax.parallel._

trait ParallelInstances {
  implicit def zioEffectForParallel(implicit ctx: ContextShift[Task]): Parallel[Task] =
    (effects: Seq[Task[_]]) => effects.toList.parSequence_
}
