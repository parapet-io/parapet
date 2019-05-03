package io.parapet

import cats.data.EitherK
import io.parapet.core.Parapet.{Flow, FlowOp, IOEffects}

package object core {

  object catsInstances {
    type CatsFlow[A] = EitherK[FlowOp, IOEffects.IOEffect, A]
    val flow = implicitly[Flow[CatsFlow]]
    val effect = implicitly[IOEffects[CatsFlow]]
  }

}
