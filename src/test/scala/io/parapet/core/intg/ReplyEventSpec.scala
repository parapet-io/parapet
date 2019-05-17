package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Parapet._
import io.parapet.core.catsInstances.effect._
import io.parapet.core.catsInstances.flow._
import org.scalatest.FlatSpec
import org.scalatest.Matchers.{empty => mempty, _}
import scala.concurrent.duration._

class ReplyEventSpec extends FlatSpec {
  import ReplyEventSpec._

  "Reply" should "send event back to sender" in {
    var responseReceived = false
    val p2 = Process[IO] {
      case Request => reply(sender => Response ~> sender)
    }

    val p1 = Process[IO] {
      case Response =>
        eval {
          responseReceived = true
        } ++ terminate
      case Start => par(Request ~> p2, delay(3.seconds, terminate))
    }

    val program = Start ~> p1

    run(program, p1, p2)

    responseReceived shouldBe true

  }

  def run(pProgram: FlowF[IO, Unit], pProcesses: Process[IO]*): Unit = {
    val app = new CatsApp {
      override val program: ProcessFlow = pProgram

      override val processes: Array[Process[IO]] = pProcesses.toArray
    }
    app.run.unsafeRunSync()
  }
}

object ReplyEventSpec {

  object Request extends Event

  object Response extends Event

}
