package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.testutils.BasicCatsIOSpec
import io.parapet.testutils.tags.CatsTest

@CatsTest
class ProcessBehaviourSpec extends io.parapet.tests.intg.ProcessBehaviourSpec[IO] with BasicCatsIOSpec