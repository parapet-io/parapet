package io.parapet.testutils

import io.parapet.ParApp
import io.parapet.core.Parapet.{ParConfig, defaultConfig}
import io.parapet.core.Process
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.testutils.TestApp._

import scala.concurrent.ExecutionContext

trait TestApp[F[_]] {

  def create(cfg: TestAppConfig[F], executionContext: ExecutionContext): ParApp[F]

}

object TestApp {

  case class TestAppConfig[F[_]](processes: F[Seq[Process[F]]],
                                 deadLetterOpt: Option[F[DeadLetterProcess[F]]] = None,
                                 parCfg: ParConfig = defaultConfig)

}