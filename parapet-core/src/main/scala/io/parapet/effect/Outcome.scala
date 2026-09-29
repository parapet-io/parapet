package io.parapet.effect

/** Terminal state of a fiber. */
sealed trait Outcome[+A]

object Outcome:
  /** The fiber completed with `value`. */
  final case class Succeeded[A](value: A) extends Outcome[A]

  /** The fiber terminated with `error`. */
  final case class Failed(error: Throwable) extends Outcome[Nothing]

  /** The fiber terminated by cancellation. */
  final case class Canceled() extends Outcome[Nothing]
