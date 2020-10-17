package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.testutils.BasicCatsIOSpec
import io.parapet.testutils.tags.CatsTest

@CatsTest
class ErrorHandlingSpec extends io.parapet.tests.intg.ErrorHandlingSpec[IO] with BasicCatsIOSpec