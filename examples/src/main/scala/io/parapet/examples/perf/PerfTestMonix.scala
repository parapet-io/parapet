package io.parapet.examples.perf

import io.parapet.MonixApp
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Scheduler.SchedulerConfig
import monix.eval.Task

object PerfTestMonix extends PerfTest[Task](1000000, 10) with MonixApp {
  override val config: ParConfig = ParConfig(-1, schedulerConfig = SchedulerConfig(10))
}
