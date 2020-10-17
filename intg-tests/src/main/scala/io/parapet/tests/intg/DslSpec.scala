package io.parapet.tests.intg

import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.Start
import io.parapet.core.{Event, Process, ProcessRef}
import io.parapet.tests.intg.DslSpec._
import io.parapet.testutils.{EventStore, IntegrationSpec}
import org.scalatest.Matchers._
import org.scalatest.OptionValues._
import org.scalatest.WordSpec

import scala.concurrent.duration._

abstract class DslSpec[F[_]] extends WordSpec with IntegrationSpec[F] {

  import dsl._

  "Event" when {
    "send" should {
      "be delivered to the receiver" in {

        val eventStore = new EventStore[F, Request]

        val consumer = Process[F](ref => {
          case req: Request => eval(eventStore.add(ref, req))
        })

        val producer = Process[F](_ => {
          case Start => Request("data") ~> consumer.ref
        })

        unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(consumer, producer))).run))
        eventStore.get(consumer.ref).headOption.value shouldBe Request("data")

      }
    }
  }

  "Events" when {
    "send" should {
      "be delivered to the receiver in send order" in {

        val eventStore = new EventStore[F, Request]

        val consumer = Process.builder[F](ref => {
          case req: Request => eval(eventStore.add(ref, req))
        }).ref(ProcessRef("consumer")).build

        val producer = Process.builder[F](_ => {
          case Start =>
            Request("1") ~> consumer.ref ++
              Request("2") ~> consumer.ref ++
              Request("3") ~> consumer.ref
        }).ref(ProcessRef("producer")).build
        unsafeRun(eventStore.await(2, createApp(ct.pure(Seq(consumer, producer))).run))
        eventStore.get(consumer.ref) shouldBe Seq(Request("1"), Request("2"), Request("3"))
      }
    }
  }

  "Event" when {
    "send to many receivers" should {
      "be delivered to all receivers in specified order" in {
        val eventStore = new EventStore[F, Request]

        def createServer(addr: String): Process[F] = Process[F](ref => {
          case req: Request => eval(eventStore.add(ref, Request(s"$addr-${req.body}")))
        })

        val serverA = createServer("A")
        val serverB = createServer("B")
        val serverC = createServer("C")

        val init = onStart(send(Request("ping"), serverA.ref, serverB.ref, serverC.ref))

        unsafeRun(eventStore.await(3, createApp(ct.pure(Seq(init, serverA, serverB, serverC))).run))
        eventStore.get(serverA.ref).headOption.value shouldBe Request("A-ping")
        eventStore.get(serverB.ref).headOption.value shouldBe Request("B-ping")
        eventStore.get(serverC.ref).headOption.value shouldBe Request("C-ping")

      }
    }
  }

  "withSender callback function" when {
    "event received" should {
      "pass sender ref to the callback function" in {

        val eventStore = new EventStore[F, Response]

        val server = Process[F](_ => {
          case Request(data) => {
            for {
              sender <- withSender(sender => eval(sender))
              _ <- Response(s"echo-$data") ~> sender
            } yield ()
          }
        })

        val client: Process[F] = Process[F](ref => {
          case Start => Request("hello") ~> server
          case res: Response => eval(eventStore.add(ref, res))
        })

        unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(server, client))).run))
        eventStore.get(client.ref).headOption.value shouldBe Response("echo-hello")

      }
    }
  }

  "Par operator" when {
    "applied to a flow" should {
      "execute operations in parallel" in {
        val eventStore = new EventStore[F, Request]

        val consumerRef = ProcessRef("consumer")
        val producerRef = ProcessRef("producer")

        val consumer = Process.builder[F](ref => {
          case req: Request => eval(eventStore.add(ref, req))
        }).ref(consumerRef).build

        val producer = Process.builder[F](_ => {
          case Start => par(
            delay(4.seconds) ++ Request("1") ~> consumerRef,
            delay(3.seconds) ++ Request("2") ~> consumerRef,
            delay(2.seconds) ++ Request("3") ~> consumerRef
          )
        }).ref(producerRef).build

        unsafeRun(eventStore.await(3, createApp(ct.pure(Seq(consumer, producer))).run))
        eventStore.get(consumer.ref) shouldBe Seq(Request("3"), Request("2"), Request("1"))

      }
    }
  }

  "Delay operator" when {
    "followed by a single operation" should {
      "delay that operation for the given duration" in {

        val eventStore = new EventStore[F, Delayed]
        val durationInMillis = 1000
        val process = Process[F](ref => {
          case Start => delay(durationInMillis.millis) ++
            eval(eventStore.add(ref, Delayed(System.currentTimeMillis())))
        })

        val start = System.currentTimeMillis()
        unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(process))).run))
        eventStore.get(process.ref).headOption.value.ts shouldBe >=(start + durationInMillis)

      }
    }
  }

  // todo not supported
  //  "Delay operator" when {
  //    "applied to a sequential flow" should {
  //      "delay every operation inside that flow for the given duration" in {
  //        val eventStore = new EventStore[F, Delayed]
  //        val durationInMillis = 1000
  //        val process = Process[F](ref => {
  //          case Start => delay(durationInMillis.millis,
  //            eval(eventStore.add(ref, Delayed(System.currentTimeMillis()))) ++
  //              eval(eventStore.add(ref, Delayed(System.currentTimeMillis()))) ++
  //              eval(eventStore.add(ref, Delayed(System.currentTimeMillis())))
  //          )
  //        })
  //
  //        val start = System.currentTimeMillis()
  //        unsafeRun(eventStore.await(3, createApp(ct.pure(Seq(process))).run))
  //        eventStore.get(process.ref).foreach { e =>
  //          e.ts shouldBe >=(start + durationInMillis)
  //        }
  //      }
  //    }
  //  }

  "Event" when {
    "send using forward" should {
      "be delivered to a receiver with original sender reference" in {
        val eventStore = new EventStore[F, Request]

        val server: Process[F] = Process[F](ref => {
          case Request(body) => withSender(sender => eval(eventStore.add(ref, Request(s"$sender-$body"))))
        })

        val proxy: Process[F] = Process[F](_ => {
          case Request(body) => forward(Request(s"proxy-$body"), server.ref)
        })

        val client: Process[F] = Process.builder[F](_ => {
          case Start => Request("ping") ~> proxy
        }).ref(ProcessRef("client")).build

        unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(client, server, proxy))).run))

        eventStore.get(server.ref).headOption.value shouldBe Request(s"client-proxy-ping")
      }
    }
  }

  "A flow" when {
    "called recursively" should {
      "be evaluated lazily" in {

        val eventStore = new EventStore[F, IntEvent]

        val process: Process[F] = Process[F](ref => {

          def times(n: Int): DslF[F, Unit] = {
            def step(remaining: Int): DslF[F, Unit] = flow {
              if (remaining == 0) unit
              else eval(eventStore.add(ref, IntEvent(remaining))) ++ step(remaining - 1)
            }

            step(n)
          }

          {
            case Start => times(5)
          }
        })
        unsafeRun(eventStore.await(5, createApp(ct.pure(Seq(process))).run))

        eventStore.get(process.ref) shouldBe (5 to 1 by -1).map(IntEvent)

      }
    }
  }

  "A flow" when {
    "forked" should {
      "should be executed concurrently" in {

        val eventStore = new EventStore[F, Request]

        val longRunningTask = delay(1.minute)

        val process: Process[F] = Process[F](ref => {
          case Start => fork(longRunningTask) ++ eval(eventStore.add(ref, Request("end")))
        })

        unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(process))).run))

      }
    }
  }

  "A two flows" when {
    "race" should {
      "cancel a loser" in {

        val eventStore = new EventStore[F, Request]

        val longRunningTask = delay(10.minutes)

        val process: Process[F] = Process[F](ref => {
          case Start =>
            for {
              res <- race(longRunningTask, eval("now"))
              _ <- res match {
                case Left(_) => suspend(ct.raiseError(new RuntimeException("longRunningTask should be canceled")))
                case Right(_) => eval(eventStore.add(ref, Request("end")))
              }
            } yield ()
        })
        unsafeRun(eventStore.await(1, createApp(ct.pure(Seq(process))).run))
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
