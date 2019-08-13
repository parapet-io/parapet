package io.parapet.tests.intg.ziotask

import io.parapet.testutils.BasicZioTaskSpec
import org.scalatest.Ignore
import scalaz.zio.Task

@Ignore
class ProcessLifecycleSpec extends io.parapet.tests.intg.ProcessLifecycleSpec[Task] with BasicZioTaskSpec