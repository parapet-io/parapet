package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.tests.intg.BasicCatsIOSpec
import io.parapet.testutils.tags.CatsTest

@CatsTest
class DynamicProcessCreationSpec extends io.parapet.tests.intg.DynamicProcessCreationSpec[IO] with BasicCatsIOSpec