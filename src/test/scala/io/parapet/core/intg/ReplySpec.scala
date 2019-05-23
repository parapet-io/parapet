package io.parapet.core.intg

import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.IO
import io.parapet.core.Parapet._
import io.parapet.core.catsInstances.effect._
import io.parapet.core.catsInstances.flow._
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class ReplySpec extends FlatSpec with IntegrationSpec {
  import ReplySpec._

  "Reply" should "send event back to sender" in {
    val responseReceived = new AtomicBoolean(false)
    val p2 = Process[IO] {
      case Request => reply(sender => Response ~> sender)
    }

    val p1 = Process[IO] {
      case Start => Request ~> p2
      case Response =>
        eval(responseReceived.compareAndSet(false, true)) ++ terminate
    }

    val program = Start ~> p1

    run(program, p1, p2)

    responseReceived.get() shouldBe true

  }

}

object ReplySpec {

  object Request extends Event

  object Response extends Event

}
