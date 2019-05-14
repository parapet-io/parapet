package io.parapet

import cats.effect.IO
import io.parapet.core.Parapet.{Effects, Flow}

package object core {

  object catsInstances {
    type CatsFlow[A] = Parapet.FlowOpOrEffect[IO, A]
    val flow = implicitly[Flow[IO, CatsFlow]]
    val effect = implicitly[Effects[IO, CatsFlow]]
    // todo val all = ???
  }

}
