package io.parapet.syntax
import io.parapet.core.Process

trait ProcessSyntax {
  implicit class ProcessOps[F[_]](p: Process[F]) {
    // alias for Free flatMap
    def ++[B](that: Process[F]): Process[F] = p.and(that)
  }
}
