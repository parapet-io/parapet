package io.parapet.core

import scala.concurrent.duration.FiniteDuration

/** Abstraction over wall-clock and high-resolution time, allowing test code to swap in a controllable [[Clock.Mock]].
  */
trait Clock {

  /** Wall-clock time in milliseconds since the epoch. */
  def currentTimeMillis: Long

  /** High-resolution monotonic time in nanoseconds. Suitable for measuring elapsed intervals; not comparable to
    * [[currentTimeMillis]].
    */
  def nanoTime: Long

}

/** Clock factories and built-in implementations. */
object Clock {

  /** Returns the singleton [[SystemClock]] backed by `java.lang.System`. */
  def apply(): Clock = SystemClock

  /** Real clock backed by `System.currentTimeMillis` / `System.nanoTime`. */
  object SystemClock extends Clock {
    override def currentTimeMillis: Long = System.currentTimeMillis()

    override def nanoTime: Long = System.nanoTime()
  }

  /** A controllable clock for tests.
    *
    * Use [[update]] to jump to a specific time or [[tick]] to advance by a delta.
    *
    * @param time
    *   the initial time, used for both millis and nanos.
    */
  class Mock(private var time: FiniteDuration) extends Clock {

    /** Sets the current time. */
    def update(value: FiniteDuration): Unit =
      time = value

    /** Advances the current time by `delta`. */
    def tick(delta: FiniteDuration): Unit =
      time = time + delta

    override def currentTimeMillis: Long =
      time.toMillis

    override def nanoTime: Long =
      time.toNanos
  }

}
