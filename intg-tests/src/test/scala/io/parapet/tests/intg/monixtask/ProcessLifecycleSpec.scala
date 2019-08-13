package io.parapet.tests.intg.monixtask

import io.parapet.testutils.BasicMonixTaskSpec
import monix.eval.Task
import org.scalatest.Ignore

@Ignore
class ProcessLifecycleSpec extends io.parapet.tests.intg.ProcessLifecycleSpec[Task] with BasicMonixTaskSpec