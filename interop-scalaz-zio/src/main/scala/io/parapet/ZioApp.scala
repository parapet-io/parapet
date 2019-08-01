package io.parapet

import cats.effect.{Concurrent, ContextShift, Timer}
import cats.{~>, Parallel => CatsParallel}
import io.parapet.core.{Context, DslInterpreter, Parallel}
import scalaz.zio.interop.ParIO
import scalaz.zio.{DefaultRuntime, Task, ZIO}
import scalaz.zio.interop.catz.implicits._
import scalaz.zio.interop.catz._
import cats.instances.list._
import cats.syntax.parallel._
import io.parapet.zioinstances.parallel._
abstract class ZioApp extends ParApp[Task] with DefaultRuntime {

  val runtime = new DefaultRuntime {}

  override val contextShift: ContextShift[Task] = ContextShift[Task]

  override val ct: Concurrent[Task] = Concurrent[Task]

  override val parallel: Parallel[Task] = Parallel[Task]

  override val timer: Timer[Task] = Timer[Task]

  override def flowInterpreter(context: Context[Task]): FlowOp ~> Flow = DslInterpreter[Task](context)

  override def unsafeRun(t: Task[Unit]): Unit = runtime.unsafeRunSync(t)
}
