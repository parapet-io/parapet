package io.parapet.testutils

import cats.effect.{Concurrent, ContextShift, Timer}
import io.parapet.core.Dsl.{DslF, WithDsl}
import io.parapet.core.Event.Start
import io.parapet.core.Parapet.{ParConfig, defaultConfig}
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.core.{Parallel, Parapet, Process, ProcessRef}
import io.parapet.syntax.FlowSyntax
import io.parapet.testutils.TestApp.TestAppConfig

import scala.concurrent.ExecutionContext

trait IntegrationSpec[F[_]] extends WithDsl[F] with FlowSyntax[F] {

  final val TestSystemRef = ProcessRef(s"${Parapet.ParapetPrefix}-test-system")

  def ec: ExecutionContext
  implicit def ctxShift: ContextShift[F]
  implicit def ct: Concurrent[F]
  implicit def timer: Timer[F]
  implicit def pa: Parallel[F]
  def testApp: TestApp[F]

  def run(processes: F[Seq[Process[F]]],
          deadLetterOpt: Option[F[DeadLetterProcess[F]]] = None,
          parCfg: ParConfig = defaultConfig): F[Unit] = {

    testApp.create(TestAppConfig(
      processes = processes,
      deadLetterOpt = deadLetterOpt,
      parCfg = parCfg
    ), ec).run

  }

  def onStart(program: DslF[F, Unit]): Process[F] = {
    Process.builder[F](_ => {
      case Start => program
    }).ref(TestSystemRef).build
  }

  def runSync(fa: F[_]): Unit

  implicit class EffectRun(fa: F[_]) {

    def unsafeRunSync(): Unit = runSync(fa)

  }

}
