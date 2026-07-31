package io.parapet.effect

import scala.concurrent.duration.*

/** A retry backoff schedule: the delay to wait before retry number `retry` (1-based). */
sealed trait Backoff:
  def duration(retry: Int): FiniteDuration

object Backoff:

  /** The same delay before every retry. */
  final case class Fixed(delay: FiniteDuration) extends Backoff:
    def duration(retry: Int): FiniteDuration = delay

  /** Exponential growth: `base * factor^(retry - 1)`, capped at `max`. */
  final case class Exp(base: FiniteDuration, factor: Double = 2.0, max: FiniteDuration = 30.seconds) extends Backoff:
    require(factor >= 1.0, "factor must be >= 1")

    def duration(retry: Int): FiniteDuration =
      val scaled = base.toMillis.toDouble * math.pow(factor, math.max(0, retry - 1))
      math.min(scaled, max.toMillis.toDouble).round.millis
