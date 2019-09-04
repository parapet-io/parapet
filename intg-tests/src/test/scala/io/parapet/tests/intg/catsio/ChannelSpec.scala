package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.core.Parapet
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Scheduler.SchedulerConfig
import io.parapet.testutils.BasicCatsIOSpec

class ChannelSpec extends io.parapet.tests.intg.ChannelSpec[IO] with BasicCatsIOSpec {
  override val config: Parapet.ParConfig = ParConfig(-1, SchedulerConfig(1))
}