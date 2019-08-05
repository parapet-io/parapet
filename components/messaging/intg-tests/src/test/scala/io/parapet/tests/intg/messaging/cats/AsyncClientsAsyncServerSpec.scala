package io.parapet.tests.intg.messaging.cats

import cats.effect.IO
import io.parapet.testutils.BasicCatsIOSpec

class AsyncClientsAsyncServerSpec
  extends io.parapet.tests.intg.messaging.AsyncClientsAsyncServerSpec[IO] with BasicCatsIOSpec
