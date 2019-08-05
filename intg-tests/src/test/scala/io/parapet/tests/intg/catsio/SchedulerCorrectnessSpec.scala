package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.core.DslInterpreter.Interpreter
import io.parapet.core.{Context, DslInterpreter}
import io.parapet.testutils.BasicCatsIOSpec

class SchedulerCorrectnessSpec extends io.parapet.tests.intg.SchedulerCorrectnessSpec[IO] with BasicCatsIOSpec {
  override def interpreter(context: Context[IO]): IO[Interpreter[IO]] =
    IO.pure(DslInterpreter[IO](context)(ct, parallel, timer))
}