package io.parapet.tests.intg

import io.parapet.core.DslInterpreter.Interpreter
import io.parapet.core.{Context, DslInterpreter}
import io.parapet.zioinstances.parallel._
import scalaz.zio.Task
import scalaz.zio.interop.catz._
import scalaz.zio.interop.catz.implicits._

class SchedulerCorrectnessZioTaskSpec extends SchedulerCorrectnessSpec[Task] with BasicZioTaskSpec {

  override def interpreter(context: Context[Task]): Task[Interpreter[Task]] =
    Task.apply(DslInterpreter[Task](context))

}
