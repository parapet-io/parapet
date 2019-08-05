package io.parapet.tests.intg.ziotask

import io.parapet.testutils.BasicZioTaskSpec
import scalaz.zio.Task

class EventDeliverySpec extends io.parapet.tests.intg.EventDeliverySpec[Task] with BasicZioTaskSpec