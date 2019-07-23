package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Event.Start
import io.parapet.core.intg.DslSpec._
import io.parapet.core.testutils.{EventStore, IntegrationSpec}
import io.parapet.core.{Event, Process}
import io.parapet.syntax.flow._
import org.scalatest.Matchers._
import org.scalatest.OptionValues._
import org.scalatest.WordSpec

import scala.concurrent.duration._

class DslSpec extends WordSpec with IntegrationSpec {

  import dsl._

  "Event" when {
    "send" should {
      "be delivered to the receiver" in {

        val eventStore = new EventStore[Request]

        val consumer = new Process[IO] {
          override def handle: Receive = {
            case req: Request => eval(eventStore.add(selfRef, req))
          }
        }

        val producer = new Process[IO] {
          override def handle: Receive = {
            case Start => Request("data") ~> consumer.selfRef
          }
        }

        eventStore.awaitSize(1, run(Seq(consumer, producer))).unsafeRunSync()
        eventStore.get(consumer.selfRef).headOption.value shouldBe Request("data")

      }
    }
  }

  "Events" when {
    "send" should {
      "be delivered to the receiver in send order" in {

        val eventStore = new EventStore[Request]

        val consumer = new Process[IO] {
          override def handle: Receive = {
            case req: Request => eval(eventStore.add(selfRef, req))
          }
        }

        val producer = new Process[IO] {
          override def handle: Receive = {
            case Start =>
              Request("1") ~> consumer.selfRef ++
                Request("2") ~> consumer.selfRef ++
                Request("3") ~> consumer.selfRef
          }
        }

        eventStore.awaitSize(3, run(Seq(consumer, producer))).unsafeRunSync()
        eventStore.get(consumer.selfRef) shouldBe Seq(Request("1"), Request("2"), Request("3"))

      }
    }
  }

  "Event" when {
    "send to many receivers" should {
      "be delivered to all receivers in specified order" in {
        val eventStore = new EventStore[Request]

        def createServer(addr: String): Process[IO] = new Process[IO] {
          override def handle: Receive = {
            case req: Request => eval(eventStore.add(selfRef, Request(s"$addr-${req.body}")))
          }
        }

        val serverA = createServer("A")
        val serverB = createServer("B")
        val serverC = createServer("C")

        val flow = send(Request("ping"), serverA.selfRef, serverB.selfRef, serverC.selfRef)

        eventStore.awaitSize(3, run(Seq(serverA, serverB, serverC), flow)).unsafeRunSync()
        eventStore.get(serverA.selfRef).headOption.value shouldBe Request("A-ping")
        eventStore.get(serverB.selfRef).headOption.value shouldBe Request("B-ping")
        eventStore.get(serverC.selfRef).headOption.value shouldBe Request("C-ping")

      }
    }
  }

  "Reply callback function" when {
    "event received" should {
      "pass sender ref to the callback function" in {

        val eventStore = new EventStore[Response]

        val server: Process[IO] = new Process[IO] {
          override def handle: Receive = {
            case Request(data) => reply(sender => Response(s"echo-$data") ~> sender)
          }
        }

        val client: Process[IO] = new Process[IO] {
          override def handle: Receive = {
            case Start => Request("hello") ~> server
            case res: Response => eval(eventStore.add(selfRef, res))
          }
        }

        eventStore.awaitSize(1, run(Seq(server, client))).unsafeRunSync()
        eventStore.get(client.selfRef).headOption.value shouldBe Response("echo-hello")

      }
    }
  }

  "Par operator" when {
    "applied to a flow" should {
      "execute operations in parallel" in {
        val eventStore = new EventStore[Request]

        val consumer = new Process[IO] {
          override def handle: Receive = {
            case req: Request => eval(eventStore.add(selfRef, req))
          }
        }

        val producer = new Process[IO] {
          override def handle: Receive = {
            case Start => par {
              delay(4.seconds, Request("1") ~> consumer.selfRef) ++
                delay(3.seconds, Request("2") ~> consumer.selfRef) ++
                delay(2.seconds, Request("3") ~> consumer.selfRef)
            }
          }
        }

        eventStore.awaitSize(3, run(Seq(consumer, producer))).unsafeRunSync()
        eventStore.get(consumer.selfRef) shouldBe Seq(Request("3"), Request("2"), Request("1"))

      }
    }
  }

  "Delay operator" when {
    "followed by a single operation" should {
      "delay that operation for the given duration" in {

        val eventStore = new EventStore[Delayed]
        val durationInMillis = 1000
        val process = new Process[IO] {
          override def handle: Receive = {
            case Start => delay(durationInMillis.millis) ++ eval(eventStore.add(selfRef, Delayed(System.currentTimeMillis())))
          }
        }

        val start = System.currentTimeMillis()
        eventStore.awaitSize(1, run(Seq(process))).unsafeRunSync()
        eventStore.get(process.selfRef).headOption.value.ts shouldBe >=(start + durationInMillis)

      }
    }
  }

  "Delay operator" when {
    "applied to a sequential flow" should {
      "delay every operation inside that flow for the given duration" in {
        val eventStore = new EventStore[Delayed]
        val durationInMillis = 1000
        val process = new Process[IO] {
          override def handle: Receive = {
            case Start => delay(durationInMillis.millis,
              eval(eventStore.add(selfRef, Delayed(System.currentTimeMillis()))) ++
                eval(eventStore.add(selfRef, Delayed(System.currentTimeMillis()))) ++
                eval(eventStore.add(selfRef, Delayed(System.currentTimeMillis())))
            )
          }
        }

        val start = System.currentTimeMillis()
        eventStore.awaitSize(3, run(Seq(process))).unsafeRunSync()
        eventStore.get(process.selfRef).foreach { e =>
          e.ts shouldBe >=(start + durationInMillis)
        }
      }
    }
  }

}

object DslSpec {

  case class Delayed(ts: Long) extends Event

  case class Request(body: Any) extends Event

  case class Response(body: Any) extends Event

}
