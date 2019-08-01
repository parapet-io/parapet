package io.parapet.catsnstances

import cats.effect.IO.ioParallel
import cats.effect.{ContextShift, IO}
import cats.instances.list._
import cats.syntax.parallel._
import io.parapet.core.Parallel

trait ParallelInstances {
  implicit def catsEffectForParallel(implicit ctx: ContextShift[IO]): Parallel[IO] =
    (effects: Seq[IO[_]]) => effects.toList.parSequence_
}
