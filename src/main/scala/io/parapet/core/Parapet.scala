package io.parapet.core

import com.typesafe.scalalogging.StrictLogging
import io.parapet.core.Scheduler.SchedulerConfig


object Parapet extends StrictLogging {

  val ParapetPrefix = "parapet"

  case class ParConfig(schedulerConfig: SchedulerConfig)

}