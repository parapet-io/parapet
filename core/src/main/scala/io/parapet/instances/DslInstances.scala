package io.parapet.instances

import cats.effect.IO
import io.parapet.core.Dsl.{Effects, FlowOps, Dsl}
import io.parapet.core.Parapet

object DslInstances {
  object catsInstances {
    val flow = implicitly[FlowOps[IO, Dsl[IO, ?]]]
    val effect = implicitly[Effects[IO, Dsl[IO, ?]]]
  }
}
