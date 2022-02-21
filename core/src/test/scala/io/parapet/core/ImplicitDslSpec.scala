package io.parapet.core

import cats.effect.IO
import io.parapet.core.Dsl.FlowOps
import org.scalatest.funsuite.AnyFunSuite

class ImplicitDslSpec extends AnyFunSuite {

  def implicitDsl[F[_] : FlowOps.Aux]: FlowOps.Aux[F] = implicitly[FlowOps.Aux[F]]

  test("find dsl instance") {
    implicitDsl[IO]
    implicitDsl[cats.Eval]
  }
}
