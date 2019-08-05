package io.parapet

import cats.effect.{Concurrent, ContextShift, Timer}
import cats.~>
import io.parapet.core.{Context, Parallel}
import monix.eval.Task

class MonixApp extends ParApp[Task] {
  override implicit def contextShift: ContextShift[Task] = ???

  override implicit def ct: Concurrent[Task] = ???

  override implicit def parallel: Parallel[Task] = ???

  override implicit def timer: Timer[Task] = ???

  override def processes: Task[Seq[core.Process[Task]]] = ???

  override def flowInterpreter(context: Context[Task]): FlowOp ~> Flow = ???

  override def unsafeRun(f: Task[Unit]): Unit = ???
}
