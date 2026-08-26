package io.parapet.tests.intg

import io.parapet.effect.ParIO
import io.parapet.journal.EventCodecRegistry
import io.parapet.{DeadLetterProcess, ParApp, ParConfig, ParIOApp, Process}

trait BasicParIOSpec extends IntegrationSpec[ParIO] with ParIOApp {
  self =>

  override def createApp(
      processes0: ParIO[Seq[Process[ParIO, ?, ?]]],
      deadLetter0: Option[ParIO[DeadLetterProcess[ParIO]]],
      config0: ParConfig,
      eventCodecs0: EventCodecRegistry
  ): ParApp[ParIO] =
    new ParIOApp {
      override val config: ParConfig = config0

      override def processes(args: Array[String]): ParIO[Seq[Process[ParIO, ?, ?]]] =
        processes0

      override def deadLetter: ParIO[DeadLetterProcess[ParIO]] =
        deadLetter0.getOrElse(super.deadLetter)

      override def eventCodecs: EventCodecRegistry = eventCodecs0
    }

  override def processes(args: Array[String]): ParIO[Seq[Process[ParIO, ?, ?]]] =
    ParIO.pure(Seq.empty)
}
