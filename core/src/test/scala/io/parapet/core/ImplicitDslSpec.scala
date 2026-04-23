package io.parapet.core

import io.parapet.core.Dsl.FlowOps
import io.parapet.effect.ParIO
import org.scalatest.funsuite.AnyFunSuite

class ImplicitDslSpec extends AnyFunSuite:
  def summonDsl[F[_]](using FlowOps.Aux[F]): FlowOps.Aux[F] =
    summon[FlowOps.Aux[F]]

  test("find DSL instances for built-in effects") {
    summonDsl[ParIO]
    summonDsl[TestUtils.Id]
  }
