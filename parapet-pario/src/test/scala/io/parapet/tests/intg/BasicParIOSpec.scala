package io.parapet.tests.intg

import io.parapet.core.Parapet
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.effect.ParIO
import io.parapet.{ParApp, ParIOApp, core}

trait BasicParIOSpec extends IntegrationSpec[ParIO] with ParIOApp {
  self =>

  override def createApp(
      processes0: ParIO[Seq[core.Process[ParIO, ?, ?]]],
      deadLetter0: Option[ParIO[DeadLetterProcess[ParIO]]],
      config0: Parapet.ParConfig
  ): ParApp[ParIO] =
    new ParIOApp {
      override val config: Parapet.ParConfig = config0

      override def processes(args: Array[String]): ParIO[Seq[core.Process[ParIO, ?, ?]]] =
        processes0

      override def deadLetter: ParIO[DeadLetterProcess[ParIO]] =
        deadLetter0.getOrElse(super.deadLetter)
    }

  override def processes(args: Array[String]): ParIO[Seq[core.Process[ParIO, ?, ?]]] =
    ParIO.pure(Seq.empty)
}
