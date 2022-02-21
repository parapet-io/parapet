package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.tests.intg.BasicCatsIOSpec
import io.parapet.testutils.tags.CatsTest

@CatsTest
class DslSpec extends io.parapet.tests.intg.DslSpec[IO] with BasicCatsIOSpec
