package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.tests.intg.BasicCatsIOSpec
import io.parapet.testutils.tags.CatsTest

@CatsTest
class SwitchBehaviorSpec extends io.parapet.tests.intg.SwitchBehaviorSpec[IO] with BasicCatsIOSpec
