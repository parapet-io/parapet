package io.parapet.testutils

import cats.effect.{ContextShift, IO, Timer}
import io.parapet.CatsApp
import io.parapet.core.Dsl.{DslF, WithDsl}
import io.parapet.core.Parapet.{ParConfig, defaultConfig}
import io.parapet.core.Process
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.syntax.FlowSyntax

import scala.concurrent.ExecutionContext.global

trait IntegrationSpec extends WithDsl[IO] with FlowSyntax[IO]{

  implicit val contextShift: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)

  def run(processes: Seq[Process[IO]], program: DslF[IO, Unit] = dsl.unit,
          deadLetterOpt: Option[DeadLetterProcess[IO]] = None,
          parCfg: ParConfig = defaultConfig): IO[Unit] = {
    new SpecApp(program, processes, deadLetterOpt, parCfg).run
  }

  class SpecApp(override val program: DslF[IO, Unit],
                ps: Seq[Process[IO]],
                deadLetterOpt: Option[DeadLetterProcess[IO]] = None,
                parCfg: ParConfig = defaultConfig) extends CatsApp {
    override def deadLetter: IO[DeadLetterProcess[IO]] =
      deadLetterOpt.map(IO.pure).getOrElse(super.deadLetter)

    override val config: ParConfig = parCfg

    override def processes: IO[Seq[Process[IO]]] = IO.pure(ps)
  }

}