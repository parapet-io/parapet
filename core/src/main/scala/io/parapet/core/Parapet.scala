package io.parapet.core

import com.typesafe.scalalogging.StrictLogging
import io.parapet.core.Scheduler.SchedulerConfig
import shapeless.{Lens, lens}

/** ∏åRÂπ∑†
  */
object Parapet extends StrictLogging {

  val Version = "0.0.1-RC5"

  case class ParConfig(
                        processBufferSize: Int,
                        schedulerConfig: SchedulerConfig,
                        devMode: Boolean = false,
                        tracingEnabled: Boolean = false,
                        eventLogEnabled: Boolean = false
  ) {
    self =>
    def enableTracing: ParConfig = self.copy(tracingEnabled = true)
    def withDevMode: ParConfig = self.copy(devMode = true)
    def enableEventLog: ParConfig = self.copy(eventLogEnabled = true)
  }

  object ParConfig {

    val default: ParConfig = ParConfig(
      processBufferSize = -1, // unbounded
      schedulerConfig = SchedulerConfig.default,
    )

    val processBufferSizeLens: Lens[ParConfig, Int] = lens[ParConfig].processBufferSize
    val enableTracingLens: Lens[ParConfig, Boolean] = lens[ParConfig].tracingEnabled

    // Scheduler config lenses
    val numberOfWorkersLens: Lens[ParConfig, Int] = lens[ParConfig].schedulerConfig.numberOfWorkers
  }

}
