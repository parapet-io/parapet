package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.core.Parapet
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.testutils.IntegrationSpec
import io.parapet.{CatsApp, ParApp, core}

trait BasicCatsIOSpec extends IntegrationSpec[IO] with CatsApp {
  override def createApp(processes0: IO[Seq[core.Process[IO]]],
                         deadLetter0: Option[IO[DeadLetterProcess[IO]]],
                         config0: Parapet.ParConfig): ParApp[IO] = new CatsApp {

    override val config: Parapet.ParConfig = config0

    override def processes: IO[Seq[core.Process[IO]]] = processes0

    override def deadLetter: IO[DeadLetterProcess[IO]] = deadLetter0.getOrElse(super.deadLetter)
  }

  override def processes: IO[Seq[core.Process[IO]]] = IO.pure(Seq.empty)

}