package io.parapet.net.processes

import io.parapet.core.Events.{Start, Stop}
import io.parapet.core.Process
import io.parapet.net.{DatagramTransport, UdpEvents}
import io.parapet.{ProcessRef, core}

import scala.concurrent.duration.*

class UdpBroadcastProcess[F[_]](
    transport: DatagramTransport[F],
    sink: ProcessRef,
    pollLimit: Int = 16,
    pollDelay: FiniteDuration = 10.millis,
    override val ref: ProcessRef = ProcessRef("net-udp-broadcast")
) extends Process[F]:

  import dsl._

  override val name: String = "udp-broadcast"

  private def receiveLoop: Program = flow {
    blocking {
      suspend(transport.receiveBatch(pollLimit)).flatMap { payloads =>
        payloads.foldLeft(unit) { (acc, payload) =>
          acc ++ UdpEvents.Message(payload) ~> sink
        }
      }
    } ++ delay(pollDelay) ++ receiveLoop
  }

  override def handle: Receive =
    {
      case Start =>
        fork(receiveLoop).void

      case UdpEvents.Send(payload) =>
        blocking {
          suspend(transport.publish(payload))
        }

      case Stop =>
        suspend(transport.close)
    }
