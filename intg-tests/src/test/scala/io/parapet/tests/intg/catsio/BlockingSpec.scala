package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.testutils.BasicCatsIOSpec
import io.parapet.testutils.tags.CatsTest
import org.scalatest.Ignore

@CatsTest
@Ignore
class BlockingSpec extends io.parapet.tests.intg.BlockingSpec[IO] with BasicCatsIOSpec {
}
