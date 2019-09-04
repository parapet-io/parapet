package io.parapet.tests.intg.messaging.cats

import cats.effect.IO
import io.parapet.testutils.BasicCatsIOSpec
import org.scalatest.Ignore

@Ignore
class AsyncClientsAsyncServerSpec
  extends io.parapet.tests.intg.messaging.AsyncClientsAsyncServerSpec[IO] with BasicCatsIOSpec
