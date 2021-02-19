package io.parapet.tests.intg.catsio

import cats.effect.IO
import io.parapet.testutils.{BasicCatsIOSpec, EventStore}
import org.scalatest.FunSuite
import io.parapet.core.{Channel, Event, Process, ProcessRef}
import io.parapet.core.Event.{ByteEvent, Start, StringEvent}
import org.scalatest.Matchers._
import scala.concurrent.duration._

class BlockingChannelWithTimeout extends FunSuite with BasicCatsIOSpec {

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
               case scala.util.Failure(Channel.ChannelInterruptedException) =>
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
