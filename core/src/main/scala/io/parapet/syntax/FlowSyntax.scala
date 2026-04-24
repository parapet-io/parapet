package io.parapet.syntax

import io.parapet.core.Dsl.{DslF, FlowOp, WithDsl}

/** Combinators on [[DslF]] programs.
  *
  * Mixed into [[io.parapet.ParApp]] and [[io.parapet.core.Process]] so that `++`, `void`, `handleError`, etc. are
  * available without explicit imports.
  */
trait FlowSyntax[F[_]] extends EventSyntax[F] with WithDsl[F]:
  extension [A](fa: DslF[F, A])
    /** Sequences `fa` then `fb`, discarding `fa`'s result. Equivalent to `fa.flatMap(_ => fb)`. */
    def ++[B](fb: => DslF[F, B]): DslF[F, B] =
      fa.flatMap(_ => fb)

    /** Discards the result of `fa`. */
    def void: DslF[F, Unit] =
      fa.map(_ => ())

    /** Recovers from an error raised inside `fa` by delegating to `f`. */
    def handleError[B >: A](f: Throwable => DslF[F, B]): DslF[F, B] =
      dsl.handleError(fa, f)

    /** Runs `fa` and ensures `f` runs afterwards regardless of outcome (try/finally). */
    def guarantee(f: => DslF[F, Unit]): DslF[F, Unit] =
      dsl.guarantee(fa, f)

    /** Runs `fa`, then `f`, returning `fa`'s result. */
    def through(f: => DslF[F, Unit]): DslF[F, A] =
      for
        value <- fa
        _     <- f
      yield value
