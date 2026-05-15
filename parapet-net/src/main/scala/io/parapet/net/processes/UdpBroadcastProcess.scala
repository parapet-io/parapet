package io.parapet.net.processes

import io.parapet.core.Events.{Start, Stop}
import io.parapet.core.Process
import io.parapet.net.{DatagramTransport, UdpEvents}
import io.parapet.{Event, ProcessRef, core}

import scala.concurrent.duration.*

/** Adapter [[Process]] exposing a [[DatagramTransport]] as an event-driven publish/receive endpoint.
  *
  * Polls the transport in a forked receive loop and forwards inbound payloads to `sink` as [[UdpEvents.Message]]
  * events. Accepts [[UdpEvents.Send]] for outbound publishes.
  *
  * @param transport
  *   the underlying datagram transport.
  * @param sink
  *   process to route inbound messages to.
  * @param pollLimit
  *   max datagrams drained per poll.
  * @param pollDelay
  *   sleep between polls - keeps the loop from busy-spinning when idle.
  * @param ref
  *   optional process address; defaults to the well-known `"net-udp-broadcast"`.
  */
class UdpBroadcastProcess[F[_]](
    transport: DatagramTransport[F],
    sink: ProcessRef[UdpEvents.Message],
    pollLimit: Int = 16,
    pollDelay: FiniteDuration = 10.millis,
    override val ref: ProcessRef[UdpEvents.Send] = ProcessRef[UdpEvents.Send]("net-udp-broadcast")
) extends Process[F, UdpEvents.Send, Event]:

  import dsl._

  override val name: String = "udp-broadcast"

  private def receiveLoop: Program = flow {
    offload {
      suspend(transport.receiveBatch(pollLimit)).flatMap { payloads =>
        payloads.foldLeft(unit) { (acc, payload) =>
          acc ++ UdpEvents.Message(payload) ~> sink
        }
      }
    } ++ delay(pollDelay) ++ receiveLoop
  }

  override def handle: Receive = {
    case Start =>
      fork(receiveLoop).void

    case UdpEvents.Send(payload) =>
      offload {
        suspend(transport.publish(payload))
      }

    case Stop =>
      suspend(transport.close)
  }
