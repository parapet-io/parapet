package io.parapet.tests.intg.catsio.processes

import cats.effect.IO
import io.parapet.core.Parapet
import io.parapet.core.Parapet.ParConfig
import io.parapet.core.Scheduler.SchedulerConfig
import io.parapet.testutils.BasicCatsIOSpec
import org.scalatest.Ignore


class BullyLeaderElectionSpec extends io.parapet.tests.intg.processes.BullyLeaderElectionSpec[IO] with BasicCatsIOSpec {
  override val config: Parapet.ParConfig = ParConfig(-1, SchedulerConfig(1))
}
