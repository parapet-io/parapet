package io.parapet.tests.intg.ziotask

import io.parapet.core.DslInterpreter.Interpreter
import io.parapet.core.{Context, DslInterpreter}
import io.parapet.testutils.BasicZioTaskSpec
import org.scalatest.Ignore
import scalaz.zio.Task

@Ignore
class SchedulerCorrectnessSpec extends io.parapet.tests.intg.SchedulerCorrectnessSpec[Task] with BasicZioTaskSpec {
  override def interpreter(context: Context[Task]): Task[Interpreter[Task]] =
    Task(DslInterpreter[Task](context)(ct, parallel, timer))
}