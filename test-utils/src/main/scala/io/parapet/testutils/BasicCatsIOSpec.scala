package io.parapet.testutils

import cats.effect.IO
import io.parapet.{CatsApp, ParApp, core}
import io.parapet.core.Parapet
import io.parapet.core.processes.DeadLetterProcess

import scala.concurrent.ExecutionContext

trait BasicCatsIOSpec extends IntegrationSpec[IO] with CatsApp { self =>
  override def createApp(processes0: IO[Seq[core.Process[IO]]],
                         deadLetter0: Option[IO[DeadLetterProcess[IO]]],
                         config0: Parapet.ParConfig): ParApp[IO] = new CatsApp {

    override lazy val ec: ExecutionContext = self.ec

    override val config: Parapet.ParConfig = config0

    override def processes(args: Array[String]): IO[Seq[core.Process[IO]]] = processes0

    override def deadLetter: IO[DeadLetterProcess[IO]] = deadLetter0.getOrElse(super.deadLetter)
  }

  override def processes(args: Array[String]): IO[Seq[core.Process[IO]]] = IO.pure(Seq.empty)

}