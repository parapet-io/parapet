package io.parapet.core

import io.parapet.core.Dsl.FlowOps
import org.scalatest.funsuite.AnyFunSuite

class ImplicitDslSpec extends AnyFunSuite:
  def summonDsl[F[_]](using FlowOps.Aux[F]): FlowOps.Aux[F] =
    summon[FlowOps.Aux[F]]

  test("find DSL instances for framework test effects") {
    summonDsl[TestUtils.Id]
    summonDsl[TestUtils.TestIO]
  }
