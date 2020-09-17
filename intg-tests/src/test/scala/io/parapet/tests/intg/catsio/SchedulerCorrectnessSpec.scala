package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.core.{Context, DslInterpreter}
import io.parapet.testutils.BasicCatsIOSpec
import org.scalatest.{Outcome, Retries}

class SchedulerCorrectnessSpec extends io.parapet.tests.intg.SchedulerCorrectnessSpec[IO]
  with BasicCatsIOSpec with Retries {
  val retries = 1

  override def withFixture(test: NoArgTest): Outcome = {
    withFixture(test, retries)
  }

  def withFixture(test: NoArgTest, count: Int): Outcome = {
    println(s"run-$count")
    val outcome = super.withFixture(test)
    outcome match {
      case _ => if (count == 1) super.withFixture(test) else withFixture(test, count - 1)
    }
  }

  override def interpreter(context: Context[IO]): IO[DslInterpreter.Interpreter[IO]] =
    IO.pure(DslInterpreter[IO](context))
}