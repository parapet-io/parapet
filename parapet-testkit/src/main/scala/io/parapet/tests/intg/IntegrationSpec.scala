package io.parapet.tests.intg

import io.parapet.core.Dsl.{DslF, WithDsl}
import io.parapet.core.Events.Start
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Process
import io.parapet.core.processes.DeadLetterProcess
import io.parapet.effect.{Effect, EffectFiber}
import io.parapet.effect.Monad.*
import io.parapet.syntax.FlowSyntax
import io.parapet.{ParApp, ProcessRef}

trait IntegrationSpec[F[_]] extends WithDsl[F] with FlowSyntax[F] with ParApp[F] {

  final val TestSystemRef = ProcessRef(s"${ProcessRef.ParapetPrefix}-test-system")

  protected object ct {
    def pure[A](value: A): F[A] =
      summon[Effect[F]].pure(value)

    def delay[A](thunk: => A): F[A] =
      summon[Effect[F]].delay(thunk)

    def start[A](fa: F[A]): F[EffectFiber[F, A]] =
      summon[Effect[F]].start(fa)

    def raiseError[A](error: Throwable): F[A] =
      summon[Effect[F]].raiseError(error)

    def unit: F[Unit] =
      summon[Effect[F]].pure(())
  }

  extension [A](fa: F[A]) {
    def >>[B](fb: => F[B]): F[B] =
      fa.flatMap(_ => fb)

    def as[B](value: => B): F[B] =
      fa.map(_ => value)

    def void: F[Unit] =
      fa.as(())
  }

  def createApp(
      processes0: F[Seq[Process[F]]],
      deadLetter0: Option[F[DeadLetterProcess[F]]] = None,
      config0: ParConfig = ParConfig.default
  ): ParApp[F]

  def onStart(program: DslF[F, Unit]): Process[F] =
    Process
      .builder[F](_ => { case Start =>
        program
      })
      .ref(TestSystemRef)
      .build

}
