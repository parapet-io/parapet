package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.core.{Context, DslInterpreter}
import io.parapet.testutils.BasicCatsIOSpec
import io.parapet.testutils.tags.CatsTest
import org.scalatest.{Outcome, Retries}

@CatsTest
class SchedulerCorrectnessSpec extends io.parapet.tests.intg.SchedulerCorrectnessSpec[IO]
  with BasicCatsIOSpec with Retries {
  val retries = 1

  override def withFixture(test: NoArgTest): Outcome = {
    withFixture(test, retries)
  }

  def withFixture(test: NoArgTest, count: Int): Outcome = {
    //println(s"run-$count")  todo logger
    val outcome = super.withFixture(test)
    outcome match {
      case _ => if (count == 1) super.withFixture(test) else withFixture(test, count - 1)
    }
  }

  override def interpreter(context: Context[IO]): DslInterpreter.Interpreter[IO] = DslInterpreter[IO](context)
}