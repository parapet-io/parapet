package io.parapet.tests.intg

import cats.effect.{Concurrent, ContextShift, Timer}
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.{ParApp, ZioApp, core}
import io.parapet.core.{Parallel, Parapet}
import io.parapet.testutils.{IntegrationSpec, TestApp}
import scalaz.zio.{DefaultRuntime, Task}
import scalaz.zio.interop.catz.implicits._
import scalaz.zio.interop.catz._
import io.parapet.zioinstances.parallel._

import scala.concurrent.ExecutionContext

trait BasicZioTaskSpec extends IntegrationSpec[Task] {

  val runtime = new DefaultRuntime {}

  override def ec: ExecutionContext = ExecutionContext.global

  override def ctxShift: ContextShift[Task] = ContextShift[Task]

  override def ct: Concurrent[Task] = Concurrent[Task]

  override def timer: Timer[Task] = Timer[Task]

  override def pa: Parallel[Task] = Parallel[Task]

  override def testApp: TestApp[Task] = new TestApp[Task] {
    override def create(cfg: TestApp.TestAppConfig[Task],
                        executionContext: ExecutionContext): ParApp[Task] = {
      new ZioApp {
        override def processes: Task[Seq[core.Process[Task]]] = cfg.processes

        override val config: Parapet.ParConfig = cfg.parCfg
        override def deadLetter: Task[DeadLetterProcess[Task]] = cfg.deadLetterOpt.getOrElse(super.deadLetter)

      }
    }
  }

  override def runSync(fa: Task[_]): Unit = runtime.unsafeRunSync(fa).getOrElse(res => {
    println(res)
  })
}
