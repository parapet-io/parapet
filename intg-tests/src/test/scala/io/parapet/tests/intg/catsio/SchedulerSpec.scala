package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.testutils.BasicCatsIOSpec
import io.parapet.testutils.tags.CatsTest

@CatsTest
class SchedulerSpec extends io.parapet.tests.intg.SchedulerSpec[IO] with BasicCatsIOSpec