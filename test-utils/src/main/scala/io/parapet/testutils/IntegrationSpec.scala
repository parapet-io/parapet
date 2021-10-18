package io.parapet.testutils

import io.parapet.core.Dsl.{DslF, WithDsl}
import io.parapet.core.Events.Start
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Process
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.syntax.FlowSyntax
import io.parapet.{ParApp, ProcessRef}

trait IntegrationSpec[F[_]] extends WithDsl[F] with FlowSyntax[F] with ParApp[F] {

  final val TestSystemRef = ProcessRef(s"${ProcessRef.ParapetPrefix}-test-system")

  def createApp(processes0: F[Seq[Process[F]]],
                deadLetter0: Option[F[DeadLetterProcess[F]]] = None,
                config0: ParConfig = ParConfig.default): ParApp[F]

  def onStart(program: DslF[F, Unit]): Process[F] = {
    Process.builder[F](_ => {
      case Start => program
    }).ref(TestSystemRef).build
  }

}