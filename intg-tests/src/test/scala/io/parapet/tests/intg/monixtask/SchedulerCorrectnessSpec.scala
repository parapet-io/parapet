package io.parapet.tests.intg.monixtask

import io.parapet.core.DslInterpreter.Interpreter
import io.parapet.core.{Context, DslInterpreter}
import io.parapet.testutils.BasicMonixTaskSpec
import monix.eval.Task

class SchedulerCorrectnessSpec extends io.parapet.tests.intg.SchedulerCorrectnessSpec[Task] with BasicMonixTaskSpec {
  override def interpreter(context: Context[Task]): Task[Interpreter[Task]] =
    Task(DslInterpreter[Task](context)(ct, timer))
}