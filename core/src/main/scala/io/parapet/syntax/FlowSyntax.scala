package io.parapet.syntax

import cats.free.Free
import io.parapet.core.Dsl.{DslF, FlowOp, WithDsl}

trait FlowSyntax[F[_]] extends EventSyntax[F] with WithDsl[F] {

  implicit class FreeOps[A](fa: DslF[F, A]) {
    // alias for Free flatMap
    def ++[B](fb: => DslF[F, B]): DslF[F, B] = fa.flatMap(_ => fb)

    def handleError[AA >: A](f: Throwable => Free[FlowOp[F, *], AA]): Free[FlowOp[F, *], AA] = dsl.handleError(fa, f)

    def finalize(f: => DslF[F, Unit]): DslF[F, A] = {
      for {
        a <- fa
        _ <- f
      } yield a
    }
  }

}
