package io.parapet.net.processes

import io.parapet.core.Events.{Start, Stop}
import io.parapet.core.Process
import io.parapet.Event
import io.parapet.net.transport.{Message, ReceiveResult, TransportError}
import io.parapet.net.transport.DatagramTransport
import io.parapet.ProcessRef

import scala.concurrent.duration.*

import DatagramProcess.*

final class DatagramProcess[F[_]](
    transport: DatagramTransport[F],
    sink: ProcessRef[Received | Failed],
    pollLimit: Int = 16,
    pollDelay: FiniteDuration = 10.millis,
    override val ref: ProcessRef[Publish] = ProcessRef[Publish]("net-datagram")
) extends Process[F, Publish, Nothing]:

  import dsl.*

  override val name: String = "net-datagram"

  private def receiveLoop: Program = flow {
    suspend(transport.receiveBatch(pollLimit)).flatMap {
      case ReceiveResult.Received(messages) =>
        messages.foldLeft(unit) { (acc, message) =>
          acc ++ (Received(message) ~> sink)
        }

      case ReceiveResult.Idle =>
        unit

      case ReceiveResult.Failed(error) =>
        Failed(error) ~> sink
    } ++ delay(pollDelay) ++ receiveLoop
  }

  override def handle: Receive = {
    case Start =>
      fork(receiveLoop).void

    case Publish(message) =>
      suspend(transport.publish(message)).flatMap {
        case Right(_)    => unit
        case Left(error) => Failed(error) ~> sink
      }

    case Stop =>
      suspend(transport.close)
  }

object DatagramProcess:
  final case class Publish(message: Message)     extends Event
  final case class Received(message: Message)    extends Event
  final case class Failed(error: TransportError) extends Event
