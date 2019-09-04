package io.parapet.tests.intg.messaging.cats

import cats.effect.IO
import io.parapet.testutils.BasicCatsIOSpec
import org.scalatest.tags.Slow

@Slow
class FLProtocolSpec extends io.parapet.tests.intg.messaging.FLProtocolSpec[IO] with BasicCatsIOSpec
