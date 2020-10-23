package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.testutils.BasicCatsIOSpec
import io.parapet.testutils.tags.CatsTest

@CatsTest
class BlockingSpec extends io.parapet.tests.intg.BlockingSpec[IO] with BasicCatsIOSpec {
}
