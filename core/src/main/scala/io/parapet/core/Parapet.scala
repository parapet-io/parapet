package io.parapet.core

import com.typesafe.scalalogging.StrictLogging
import io.parapet.core.Scheduler.SchedulerConfig
import shapeless.{Lens, lens}

/**
  * ∏åRÂπ∑†
  */
object Parapet extends StrictLogging {

  val ParapetPrefix = "parapet"

  case class ParConfig(schedulerConfig: SchedulerConfig)

  // Config lenses
  val processQueueSizeLens: Lens[ParConfig, Int] = lens[ParConfig].schedulerConfig.processQueueSize

  val defaultConfig: ParConfig = ParConfig(
    schedulerConfig = SchedulerConfig(
      queueSize = -1,
      numberOfWorkers = Runtime.getRuntime.availableProcessors(),
      processQueueSize = -1))

}