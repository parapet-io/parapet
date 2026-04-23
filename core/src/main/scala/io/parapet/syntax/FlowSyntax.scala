package io.parapet.syntax

import io.parapet.core.Dsl.{DslF, FlowOp, WithDsl}

trait FlowSyntax[F[_]] extends EventSyntax[F] with WithDsl[F]:
  extension [A](fa: DslF[F, A])
    def ++[B](fb: => DslF[F, B]): DslF[F, B] =
      fa.flatMap(_ => fb)

    def void: DslF[F, Unit] =
      fa.map(_ => ())

    def handleError[B >: A](f: Throwable => DslF[F, B]): DslF[F, B] =
      dsl.handleError(fa, f)

    def guarantee(f: => DslF[F, Unit]): DslF[F, Unit] =
      dsl.guarantee(fa, f)

    def through(f: => DslF[F, Unit]): DslF[F, A] =
      for
        value <- fa
        _ <- f
      yield value
