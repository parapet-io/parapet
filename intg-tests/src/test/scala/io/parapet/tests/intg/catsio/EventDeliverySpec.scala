package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.testutils.BasicCatsIOSpec
import io.parapet.testutils.tags.CatsTest

@CatsTest
class EventDeliverySpec extends io.parapet.tests.intg.EventDeliverySpec[IO] with BasicCatsIOSpec