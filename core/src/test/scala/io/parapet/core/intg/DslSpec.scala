package io.parapet.core.intg

import cats.effect.IO
import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.Start
import io.parapet.core.intg.DslSpec._
import io.parapet.core.testutils.{EventStore, IntegrationSpec}
import io.parapet.core.{Event, Process, ProcessRef}
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

        val consumer = Process[IO](ref => {
          case req: Request => eval(eventStore.add(ref, req))
        })

        val producer = Process[IO](_ => {
          case Start => Request("data") ~> consumer.ref
        })

        eventStore.awaitSize(1, run(Seq(consumer, producer))).unsafeRunSync()
        eventStore.get(consumer.ref).headOption.value shouldBe Request("data")

      }
    }
  }

  "Events" when {
    "send" should {
      "be delivered to the receiver in send order" in {

        val eventStore = new EventStore[Request]

        val consumer = Process[IO](ref => {
          case req: Request => eval(eventStore.add(ref, req))
        })

        val producer = Process[IO](_ => {
          case Start =>
            Request("1") ~> consumer.ref ++
              Request("2") ~> consumer.ref ++
              Request("3") ~> consumer.ref
        })
        eventStore.awaitSize(3, run(Seq(consumer, producer))).unsafeRunSync()
        eventStore.get(consumer.ref) shouldBe Seq(Request("1"), Request("2"), Request("3"))

      }
    }
  }

  "Event" when {
    "send to many receivers" should {
      "be delivered to all receivers in specified order" in {
        val eventStore = new EventStore[Request]

        def createServer(addr: String): Process[IO] = Process[IO](ref => {
          case req: Request => eval(eventStore.add(ref, Request(s"$addr-${req.body}")))
        })

        val serverA = createServer("A")
        val serverB = createServer("B")
        val serverC = createServer("C")

        val flow = send(Request("ping"), serverA.ref, serverB.ref, serverC.ref)

        eventStore.awaitSize(3, run(Seq(serverA, serverB, serverC), flow)).unsafeRunSync()
        eventStore.get(serverA.ref).headOption.value shouldBe Request("A-ping")
        eventStore.get(serverB.ref).headOption.value shouldBe Request("B-ping")
        eventStore.get(serverC.ref).headOption.value shouldBe Request("C-ping")

      }
    }
  }

  "withSender callback function" when {
    "event received" should {
      "pass sender ref to the callback function" in {

        val eventStore = new EventStore[Response]

        val server = Process[IO](_ => {
          case Request(data) => withSender(sender => Response(s"echo-$data") ~> sender)
        })

        val client: Process[IO] = Process[IO](ref => {
          case Start => Request("hello") ~> server
          case res: Response => eval(eventStore.add(ref, res))
        })

        eventStore.awaitSize(1, run(Seq(server, client))).unsafeRunSync()
        eventStore.get(client.ref).headOption.value shouldBe Response("echo-hello")

      }
    }
  }

  "Par operator" when {
    "applied to a flow" should {
      "execute operations in parallel" in {
        val eventStore = new EventStore[Request]

        val consumer = Process[IO](ref => {
          case req: Request => eval(eventStore.add(ref, req))
        })

        val producer = Process[IO](_ => {
          case Start => par {
            delay(4.seconds, Request("1") ~> consumer.ref) ++
              delay(3.seconds, Request("2") ~> consumer.ref) ++
              delay(2.seconds, Request("3") ~> consumer.ref)
          }
        })

        eventStore.awaitSize(3, run(Seq(consumer, producer))).unsafeRunSync()
        eventStore.get(consumer.ref) shouldBe Seq(Request("3"), Request("2"), Request("1"))

      }
    }
  }

  "Delay operator" when {
    "followed by a single operation" should {
      "delay that operation for the given duration" in {

        val eventStore = new EventStore[Delayed]
        val durationInMillis = 1000
        val process = Process[IO](ref => {
          case Start => delay(durationInMillis.millis) ++
            eval(eventStore.add(ref, Delayed(System.currentTimeMillis())))
        })

        val start = System.currentTimeMillis()
        eventStore.awaitSize(1, run(Seq(process))).unsafeRunSync()
        eventStore.get(process.ref).headOption.value.ts shouldBe >=(start + durationInMillis)

      }
    }
  }

  "Delay operator" when {
    "applied to a sequential flow" should {
      "delay every operation inside that flow for the given duration" in {
        val eventStore = new EventStore[Delayed]
        val durationInMillis = 1000
        val process = Process[IO](ref => {
          case Start => delay(durationInMillis.millis,
            eval(eventStore.add(ref, Delayed(System.currentTimeMillis()))) ++
              eval(eventStore.add(ref, Delayed(System.currentTimeMillis()))) ++
              eval(eventStore.add(ref, Delayed(System.currentTimeMillis())))
          )
        })

        val start = System.currentTimeMillis()
        eventStore.awaitSize(3, run(Seq(process))).unsafeRunSync()
        eventStore.get(process.ref).foreach { e =>
          e.ts shouldBe >=(start + durationInMillis)
        }
      }
    }
  }

  "Event" when {
    "send using forward" should {
      "be delivered to a receiver with original sender reference" in {
        val eventStore = new EventStore[Request]

        val server: Process[IO] = Process[IO](ref => {
          case Request(body) => withSender(sender => eval(eventStore.add(ref, Request(s"$sender-$body"))))
        })

        val proxy: Process[IO] = Process[IO](_ => {
          case Request(body) => forward(Request(s"proxy-$body"), server.ref)
        })

        val client: Process[IO] = Process.builder[IO](_ => {
          case Start => Request("ping") ~> proxy
        }).ref(ProcessRef("client")).build

        eventStore.awaitSize(1, run(Seq(client, server, proxy))).unsafeRunSync()

        eventStore.get(server.ref).headOption.value shouldBe Request(s"client-proxy-ping")
      }
    }
  }

  "A flow" when {
    "called recursively" should {
      "be evaluated lazily" in {

        val eventStore = new EventStore[IntEvent]

        val process: Process[IO] = Process[IO](ref => {

          def times(n: Int): DslF[IO, Unit] = {
            def step(remaining: Int): DslF[IO, Unit] = flow {
              if (remaining == 0) unit
              else eval(eventStore.add(ref, IntEvent(remaining))) ++ step(remaining - 1)
            }

            step(n)
          }

          {
            case Start => times(5)
          }
        })
        eventStore.awaitSize(5, run(Seq(process))).unsafeRunSync()

        eventStore.get(process.ref) shouldBe (5 to 1 by -1).map(IntEvent)

      }
    }
  }

  "A process" when {
    "explicitly calls another process" should {
      "provide the same behaviour as send" in {
        val eventStore = new EventStore[Response]

        val server: Process[IO] = Process[IO](_ => {
          case Request(data) => withSender(sender => Response(s"echo-$data") ~> sender)
        })

        val client: Process[IO] = Process[IO](ref => {
          case Start => server(ref, Request("hello"))
          case res: Response => eval(eventStore.add(ref, res))
        })
        eventStore.awaitSize(1, run(Seq(client, server))).unsafeRunSync()
        eventStore.get(client.ref).headOption.value shouldBe Response("echo-hello")
      }
    }
  }

  "A flow" when {
    "forked" should {
      "should be executed concurrently" in {

        val eventStore = new EventStore[Request]

        val forever = eval(while (true) {})

        val process: Process[IO] = Process[IO](ref => {
          case Start => fork(forever) ++ eval(eventStore.add(ref, Request("end")))
        })

        eventStore.awaitSize(1, run(Seq(process))).unsafeRunSync()

      }
    }
  }


  "A two flows" when {
    "race" should {
      "cancel a loser" in {

        val eventStore = new EventStore[Request]

        val forever = eval(while (true) {})

        val process: Process[IO] = Process[IO](ref => {
          case Start => race(forever, eval(eventStore.add(ref, Request("end"))))
        })
        eventStore.awaitSize(1, run(Seq(process))).unsafeRunSync()
      }
    }
  }

  "a" when {
    "b" should {
      "c" in {

        val server: Process[IO] = Process[IO](_ => {
          case Request(data) => withSender(sender => Response(s"echo-$data") ~> sender)
        })

        val client = Process[IO](ref => {
          case Start => server(ref, Request("hello"))
          case res: Response => eval(println(res))
        })
      }
    }
  }

}

object DslSpec {

  case class Delayed(ts: Long) extends Event

  case class Request(body: Any) extends Event

  case class Response(body: Any) extends Event

  case class IntEvent(i: Int) extends Event

}
