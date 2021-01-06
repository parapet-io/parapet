package io.parapet.testutils

import io.parapet.core.Parapet
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.{MonixApp, ParApp, core}
import monix.eval.Task

import scala.concurrent.ExecutionContext

trait BasicMonixTaskSpec extends IntegrationSpec[Task] with MonixApp { self =>

  override def createApp(processes0: Task[Seq[core.Process[Task]]],
                         deadLetter0: Option[Task[DeadLetterProcess[Task]]],
                         config0: Parapet.ParConfig): ParApp[Task] = new MonixApp {

    override lazy val ec: ExecutionContext = self.ec

    override val config: Parapet.ParConfig = config0

    override def processes(args: Array[String]): Task[Seq[core.Process[Task]]] = processes0

    override def deadLetter: Task[DeadLetterProcess[Task]] = deadLetter0.getOrElse(super.deadLetter)
  }

  override def processes(args: Array[String]): Task[Seq[core.Process[Task]]] = Task.pure(Seq.empty)

}