package io.parapet.core

import com.typesafe.scalalogging.StrictLogging
import io.parapet.core.Scheduler.SchedulerConfig

object Parapet extends StrictLogging:
  val Version = "0.0.1-RC7"

  final case class ParConfig(
      processBufferSize: Int,
      schedulerConfig: SchedulerConfig,
      devMode: Boolean = false,
      tracingEnabled: Boolean = false,
      eventLogEnabled: Boolean = false
  ):
    def withProcessBufferSize(value: Int): ParConfig =
      copy(processBufferSize = value)

    def withWorkerCount(value: Int): ParConfig =
      copy(schedulerConfig = schedulerConfig.copy(numberOfWorkers = value))

    def enableTracing: ParConfig =
      copy(tracingEnabled = true)

    def withDevMode: ParConfig =
      copy(devMode = true)

    def enableEventLog: ParConfig =
      copy(eventLogEnabled = true)

  object ParConfig:
    val default: ParConfig =
      ParConfig(
        processBufferSize = -1,
        schedulerConfig = SchedulerConfig.default
      )
