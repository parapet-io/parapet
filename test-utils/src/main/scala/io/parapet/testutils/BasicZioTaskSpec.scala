package io.parapet.testutils

import io.parapet.core.Parapet
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.{ParApp, ZioApp, core}
import scalaz.zio.Task

trait BasicZioTaskSpec extends IntegrationSpec[Task] with ZioApp {

  override def createApp(processes0: Task[Seq[core.Process[Task]]],
                         deadLetter0: Option[Task[DeadLetterProcess[Task]]],
                         config0: Parapet.ParConfig): ParApp[Task] = new ZioApp {

    override val config: Parapet.ParConfig = config0

    override def processes: Task[Seq[core.Process[Task]]] = processes0

    override def deadLetter: Task[DeadLetterProcess[Task]] = deadLetter0.getOrElse(super.deadLetter)

  }

  override def processes: Task[Seq[core.Process[Task]]] = Task.apply(Seq.empty)

}