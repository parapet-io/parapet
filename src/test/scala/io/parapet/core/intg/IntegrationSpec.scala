package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Parapet.{CatsApp, FlowF, ParApp, ParConfig}
import io.parapet.core.Process
import io.parapet.core.processes.DeadLetterProcess

import scala.concurrent.duration._

trait IntegrationSpec {

  def run(program: FlowF[IO, Unit], processes: Process[IO]*): Unit = {
    new SpecApp(program, processes.toArray).unsafeRun()
  }

  class SpecApp(val program: FlowF[IO, Unit],
                val processes: Array[Process[IO]],
                deadLetterOpt: Option[DeadLetterProcess[IO]] = None,
                configOpt: Option[ParConfig] = None) extends CatsApp {
    override def deadLetter: DeadLetterProcess[IO] =
      deadLetterOpt.getOrElse(super.deadLetter)
    override val config: ParConfig = configOpt.getOrElse(ParApp.defaultConfig)

    def unsafeRun(timeout: FiniteDuration = 1.minute): Unit = {
      run.unsafeRunTimed(timeout).getOrElse(throw new RuntimeException("Test failed by timeout"))
    }
  }

}
