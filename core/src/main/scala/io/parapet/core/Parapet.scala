package io.parapet.core

import com.typesafe.scalalogging.StrictLogging
import io.parapet.core.Scheduler.SchedulerConfig
import shapeless.{Lens, lens}

/** ∏åRÂπ∑†
  */
object Parapet extends StrictLogging {

  val ParapetPrefix = "parapet"

  case class ParConfig(
      processBufferSize: Int,
      schedulerConfig: SchedulerConfig,
      devMode: Boolean = false,
      tracing: Boolean = false,
  )

  object ParConfig {

    val default: ParConfig = ParConfig(
      processBufferSize = -1, // unbounded
      schedulerConfig = SchedulerConfig.default,
    )

    val processBufferSizeLens: Lens[ParConfig, Int] = lens[ParConfig].processBufferSize
    val tracingLens: Lens[ParConfig, Boolean] = lens[ParConfig].tracing

    // Scheduler config lenses
    val numberOfWorkersLens: Lens[ParConfig, Int] = lens[ParConfig].schedulerConfig.numberOfWorkers
  }

}
