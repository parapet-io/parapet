package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.Event.{ByteEvent, StringEvent}
import io.parapet.core.Events.Start
import io.parapet.core.{Channel, Process}
import io.parapet.tests.intg.BasicCatsIOSpec
import io.parapet.testutils.EventStore
import io.parapet.{Event, ProcessRef}
import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.duration._

@Ignore
class BlockingChannelWithTimeout extends AnyFunSuite with BasicCatsIOSpec {

  import dsl._

  test("blockingChannelTimeout") {

    val eventStore = new EventStore[IO, Event]

    val server = Process.builder[IO](_ => {
      case e:ByteEvent => eval(println(s"server received: $e")) ++ delay(10.seconds) ++
        withSender(sender => ByteEvent("res".getBytes) ~> sender)
    }).ref(ProcessRef("server")).build

    val failover = Process.builder[IO](ref => {
      case e: ByteEvent => withSender(sender => eval(println(s"failover received: $e from $sender"))) ++
      eval(eventStore.add(ref, StringEvent("success")))
    }).ref(ProcessRef("failover")).build

    val clientRef = ProcessRef("client")
    val ch = new Channel[IO](clientRef)
    println(s"cannel ref: ${ch.ref}")
    val client = Process.builder[IO](ref => {
        case Start => register(ref, ch) ++
         blocking {
           race(
             ch.send(ByteEvent("request".getBytes()), server.ref,  {
               case scala.util.Failure(Channel.ChannelInterruptedException(_, _)) =>
                 eval(println("channel was interrupted")) ++ ByteEvent("help".getBytes()) ~> failover
               case res => eval(println(s"client received: $res"))
             }),
             delay(3.seconds) ++ eval(println("server doesn't respond"))) ++
             ch.send(ByteEvent("request".getBytes()), failover.ref, _ => unit)
         }
      }
    ).ref(clientRef).build

    unsafeRun(eventStore.await(2, createApp(ct.pure(Seq(client, server, failover))).run))
    eventStore.get(failover.ref) shouldBe Seq(StringEvent("success"), StringEvent("success"))

  }

}
