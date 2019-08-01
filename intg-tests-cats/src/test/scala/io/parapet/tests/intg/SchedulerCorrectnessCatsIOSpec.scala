package io.parapet.tests.intg

import cats.effect.IO
import io.parapet.catsnstances.parallel._
import io.parapet.core.DslInterpreter.Interpreter
import io.parapet.core.{Context, DslInterpreter}

class SchedulerCorrectnessCatsIOSpec
  extends SchedulerCorrectnessSpec[IO] with BasicCatsIOSpec {

  override def interpreter(context: Context[IO]): IO[Interpreter[IO]] =
    IO.pure(DslInterpreter[IO](context))

}