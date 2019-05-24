package io.parapet.core.intg

import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.IO
import io.parapet.core.Parapet._
import io.parapet.core.catsInstances.effect._
import io.parapet.core.catsInstances.flow.{empty => emptyFlow, _}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class ReplySpec extends FlatSpec with IntegrationSpec {

  import ReplySpec._

  "Reply" should "send event back to sender" in {
    val responseReceived = new AtomicBoolean(false)
    val server = Process.named[IO]("server", _ => {
      case Request => reply(sender => Response ~> sender)
    })

    val client = Process.named[IO]("client", _ => {
      case Start => Request ~> server
      case Response =>
        eval {
          if (!responseReceived.compareAndSet(false, true)) {
            throw new IllegalStateException("responseReceived must be false")
          }
        } ++ terminate
    })

    println(client)
    println(server)

    run(emptyFlow, client, server)

    responseReceived.get() shouldBe true

  }

}

object ReplySpec {

  object Request extends Event

  object Response extends Event

}
