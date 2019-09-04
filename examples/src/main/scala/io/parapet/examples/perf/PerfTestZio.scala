package io.parapet.examples.perf

import io.parapet.ZioApp
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Scheduler.SchedulerConfig
import scalaz.zio.Task

object PerfTestZio extends PerfTest[Task](1000000, 10) with ZioApp {
  override val config: ParConfig = ParConfig(-1, schedulerConfig = SchedulerConfig(10))
}