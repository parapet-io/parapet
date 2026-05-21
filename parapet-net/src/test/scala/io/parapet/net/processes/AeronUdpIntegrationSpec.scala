package io.parapet.net.processes

import cats.effect.IO
import io.parapet.core.Events.Start
import io.parapet.core.Process
import io.parapet.net.transport.Message
import io.parapet.net.transport.aeron.{AeronUdpConfig, AeronUdpTransport}
import io.parapet.tests.intg.cats.BasicCatsEffectSpec
import io.parapet.testutils.EventStore
import io.parapet.{Event, ProcessRef}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*

import scala.concurrent.duration.*

class AeronUdpIntegrationSpec extends AnyFlatSpec with BasicCatsEffectSpec:

  import dsl.*

  "AeronUdpTransport via DatagramProcess" should "deliver published payloads to the sink (loopback)" in {
    val port   = AeronUdpIntegrationSpec.freeUdpPort()
    val config = AeronUdpConfig(
      channel = s"aeron:udp?endpoint=localhost:$port",
      streamId = 1001,
      embeddedMediaDriver = true,
      directoryName = Some(s"${System.getProperty("java.io.tmpdir")}/aeron-${java.util.UUID.randomUUID()}")
    )

    val store   = new EventStore[IO, Event]
    val sinkRef = ProcessRef[DatagramProcess.Received | DatagramProcess.Failed]("udp-sink")

    unsafeRun {
      AeronUdpTransport.make[IO](config).use { transport =>
        val udpProc = new DatagramProcess[IO](transport, sinkRef, pollLimit = 16, pollDelay = 10.millis)

        val sink = new Process[IO, DatagramProcess.Received | DatagramProcess.Failed, Event] {
          override val ref: ProcessRef[DatagramProcess.Received | DatagramProcess.Failed] = sinkRef
          override def handle: Receive                                                    = {
            case message @ DatagramProcess.Received(_) => eval(store.add(ref, message))
            case failed @ DatagramProcess.Failed(_)    => eval(store.add(ref, failed))
          }
        }

        val driver = new Process[IO, Event, Event] {
          override val ref: ProcessRef[Event] = ProcessRef("driver")
          override def handle: Receive        = { case Start =>
            // Aeron publication needs a brief warmup before offer() succeeds.
            delay(500.millis) ++ DatagramProcess.Publish(Message.single("hello".getBytes("UTF-8"))) ~> udpProc.ref
          }
        }

        store.await(
          expectedSize = 1,
          op = createApp(ct.pure(Seq(udpProc, sink, driver))).run,
          timeout = 15.seconds
        )
      }
    }

    val msgs = store.get(sinkRef)
    msgs should have size 1
    msgs.head match
      case DatagramProcess.Received(message) =>
        new String(message.parts.head, "UTF-8") shouldBe "hello"
      case other =>
        fail(s"unexpected datagram event: $other")
  }

object AeronUdpIntegrationSpec:
  def freeUdpPort(): Int =
    val socket = new java.net.DatagramSocket(0)
    try socket.getLocalPort
    finally socket.close()
