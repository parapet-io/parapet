package io.parapet.tests.intg.cats

import cats.effect.IO
import io.parapet.cats.CatsEffectParApp
import io.parapet.core.Parapet
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.tests.intg.IntegrationSpec
import io.parapet.{ParApp, core}

trait BasicCatsEffectSpec extends IntegrationSpec[IO] with CatsEffectParApp:
  override def createApp(
      processes0: IO[Seq[core.Process[IO]]],
      deadLetter0: Option[IO[DeadLetterProcess[IO]]],
      config0: Parapet.ParConfig
  ): ParApp[IO] =
    new CatsEffectParApp:
      override val config: Parapet.ParConfig = config0

      override def processes(args: Array[String]): IO[Seq[core.Process[IO]]] =
        processes0

      override def deadLetter: IO[DeadLetterProcess[IO]] =
        deadLetter0.getOrElse(super.deadLetter)

  override def processes(args: Array[String]): IO[Seq[core.Process[IO]]] =
    IO.pure(Seq.empty)

//class BlockingSpec extends io.parapet.tests.intg.BlockingSpec[IO] with BasicCatsEffectSpec

//class ChannelSpec extends io.parapet.tests.intg.ChannelSpec[IO] with BasicCatsEffectSpec

class DslSpec extends io.parapet.tests.intg.DslSpec[IO] with BasicCatsEffectSpec

class DynamicProcessCreationSpec extends io.parapet.tests.intg.DynamicProcessCreationSpec[IO] with BasicCatsEffectSpec

class ErrorHandlingSpec extends io.parapet.tests.intg.ErrorHandlingSpec[IO] with BasicCatsEffectSpec

class EventDeliverySpec extends io.parapet.tests.intg.EventDeliverySpec[IO] with BasicCatsEffectSpec

class ProcessBehaviourSpec extends io.parapet.tests.intg.ProcessBehaviourSpec[IO] with BasicCatsEffectSpec

class ProcessLifecycleSpec extends io.parapet.tests.intg.ProcessLifecycleSpec[IO] with BasicCatsEffectSpec

class ProcessSpec extends io.parapet.tests.intg.ProcessSpec[IO] with BasicCatsEffectSpec

class ReplySpec extends io.parapet.tests.intg.ReplySpec[IO] with BasicCatsEffectSpec

class SchedulerCorrectnessSpec
    extends io.parapet.tests.intg.scheduler.SchedulerCorrectnessSpec[IO]
    with BasicCatsEffectSpec

class SchedulerStressSpec extends io.parapet.tests.intg.scheduler.SchedulerStressSpec[IO] with BasicCatsEffectSpec

class SchedulerSpec extends io.parapet.tests.intg.scheduler.SchedulerSpec[IO] with BasicCatsEffectSpec

class SelfSendSpec extends io.parapet.tests.intg.SelfSendSpec[IO] with BasicCatsEffectSpec

class SwitchBehaviorSpec extends io.parapet.tests.intg.SwitchBehaviorSpec[IO] with BasicCatsEffectSpec
