package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.testutils.BasicCatsIOSpec
import org.scalatest.Ignore

@Ignore
class ProcessLifecycleSpec extends io.parapet.tests.intg.ProcessLifecycleSpec[IO] with BasicCatsIOSpec