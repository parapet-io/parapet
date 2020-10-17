package io.parapet.syntax

import io.parapet.core.Dsl.DslF

trait FlowSyntax[F[_]] extends EventSyntax[F] {

  implicit class FreeOps[A](fa: DslF[F, A]) {
    // alias for Free flatMap
    def ++[B](fb: => DslF[F, B]): DslF[F, B] = fa.flatMap(_ => fb)
  }

}