package io.parapet.tests.intg.monixtask

import io.parapet.tests.intg.BasicMonixTaskSpec
import monix.eval.Task


class SelfSendSpec extends io.parapet.tests.intg.SelfSendSpec[Task] with BasicMonixTaskSpec