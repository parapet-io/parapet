package io.parapet.core.testutils

import cats.effect.{ContextShift, IO, Timer}
import io.parapet.CatsApp
import io.parapet.core.Dsl.DslF
import io.parapet.core.Parapet.{ParConfig, defaultConfig}
import io.parapet.core.Process
import io.parapet.core.processes.DeadLetterProcess

import scala.concurrent.ExecutionContext.global

trait IntegrationSpec {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)

  def run(program: DslF[IO, Unit], processes: Array[Process[IO]],
          deadLetterOpt: Option[DeadLetterProcess[IO]] = None,
          parCfg: ParConfig = defaultConfig): IO[Unit] = {
    new SpecApp(program, processes, deadLetterOpt, parCfg).run
  }

  class SpecApp(override val program: DslF[IO, Unit],
                ps: Seq[Process[IO]],
                deadLetterOpt: Option[DeadLetterProcess[IO]] = None,
                parCfg: ParConfig = defaultConfig) extends CatsApp {
    override def deadLetter: DeadLetterProcess[IO] =
      deadLetterOpt.getOrElse(super.deadLetter)

    override val config: ParConfig = parCfg

    override def processes: IO[Seq[Process[IO]]] = IO.pure(ps)
  }

}
