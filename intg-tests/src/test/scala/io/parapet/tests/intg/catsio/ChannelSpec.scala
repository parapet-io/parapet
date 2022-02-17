package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.core.Parapet
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Scheduler.SchedulerConfig
import io.parapet.tests.intg.BasicCatsIOSpec
import io.parapet.testutils.tags.CatsTest
import org.scalatest.Ignore

@CatsTest
@Ignore
class ChannelSpec extends io.parapet.tests.intg.ChannelSpec[IO] with BasicCatsIOSpec {
  override val config: Parapet.ParConfig = ParConfig(-1, SchedulerConfig(1))
}