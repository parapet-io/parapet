package io.parapet.core

import scala.concurrent.duration.FiniteDuration

trait Clock {

  def currentTimeMillis: Long

  def nanoTime: Long

}

object Clock {

  def apply(): Clock = SystemClock

  object SystemClock extends Clock {
    override def currentTimeMillis: Long = System.currentTimeMillis()

    override def nanoTime: Long = System.nanoTime()
  }

  class Mock(private var time: FiniteDuration) extends Clock {

    def update(value: FiniteDuration): Unit = {
      time = value
    }

    // number of units to add to the current value
    def tick(delta: FiniteDuration): Unit = {
      time = time + delta
    }

    override def currentTimeMillis: Long = {
      time.toMillis
    }

    override def nanoTime: Long = {
      time.toNanos
    }
  }

}
