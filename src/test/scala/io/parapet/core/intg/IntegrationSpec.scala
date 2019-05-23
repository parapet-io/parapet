package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Parapet.{CatsApp, DeadLetterProcess, FlowF, Process}

import scala.concurrent.duration._

trait IntegrationSpec {

  def run(program: FlowF[IO, Unit], processes: Process[IO]*): Unit = {
    new SpecApp(program, processes.toArray).unsafeRun()
  }

  class SpecApp(val program: FlowF[IO, Unit],
                val processes: Array[Process[IO]],
                deadLetterOpt: Option[DeadLetterProcess[IO]] = None) extends CatsApp {
    override def deadLetter: DeadLetterProcess[IO] =
      deadLetterOpt.getOrElse(super.deadLetter)


    def unsafeRun(timeout: FiniteDuration = 1.minute): Unit = {
      run.unsafeRunTimed(timeout).getOrElse(throw new RuntimeException("Test failed by timeout"))
    }
  }

}
