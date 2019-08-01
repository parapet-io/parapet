package io.parapet.tests.intg.instances

import cats.effect.IO
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.testutils.TestApp
import io.parapet.{CatsApp, ParApp, core}

import scala.concurrent.ExecutionContext

trait TestAppInstances {

  implicit val testIOApp: TestApp[IO] = new TestApp[IO] {
    override def create(cfg: TestApp.TestAppConfig[IO], executionContext: ExecutionContext): ParApp[IO] = {
      new CatsApp {

        override lazy val ec: ExecutionContext = executionContext

        override def processes: IO[Seq[core.Process[IO]]] = cfg.processes

        override def deadLetter: IO[DeadLetterProcess[IO]] = cfg.deadLetterOpt.getOrElse(super.deadLetter)

        override val config: ParConfig = cfg.parCfg
      }
    }
  }

}
