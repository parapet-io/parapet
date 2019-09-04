package io.parapet.examples.perf

import cats.effect.IO
import io.parapet.CatsApp
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Scheduler.SchedulerConfig

object PerfTestCats extends PerfTest[IO](1000000, 10) with CatsApp {

  override val config: ParConfig = ParConfig(-1, schedulerConfig = SchedulerConfig(10))

}
