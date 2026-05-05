package io.parapet.tests.intg.scheduler

import scala.concurrent.duration.{FiniteDuration, *}
import scala.util.Random

/** Controls how long a receiver spends in its handler per event.
  */
trait TaskProcessingTime {
  val name: String

  /** Duration the handler delays before finishing. */
  def time: FiniteDuration
}

object TaskProcessingTime {

  /** Fixed delay - every call to [[time]] returns the same duration. */
  final class Interval(value: FiniteDuration) extends TaskProcessingTime {
    override val name: String         = "interval"
    override def time: FiniteDuration = value.toMillis.millis
  }

  /** Uniform random delay in `[from, to]`. Each call re-samples. */
  final class Range(from: FiniteDuration, to: FiniteDuration) extends TaskProcessingTime {
    private val rnd                   = new Random
    override val name: String         = s"range[$from, $to]"
    override def time: FiniteDuration = {
      val fromMillis = from.toMillis
      val toMillis   = to.toMillis
      (fromMillis + rnd.nextInt((toMillis - fromMillis).toInt + 1)).millis
    }
  }

  /** Zero-cost handler - returns immediately. */
  val instant: TaskProcessingTime = new TaskProcessingTime {
    private val now                   = 0.millis
    override val name: String         = "instant"
    override def time: FiniteDuration = now
  }

  def range(from: FiniteDuration, to: FiniteDuration): TaskProcessingTime = new Range(from, to)

  def interval(time: FiniteDuration): TaskProcessingTime = new Interval(time)

}
