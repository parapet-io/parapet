package io.parapet.net.processes

import io.parapet.core.Events.{Start, Stop}
import io.parapet.core.Process
import io.parapet.Event
import io.parapet.net.transport.{Message, ReceiveResult, RoutedMessage, RoutingId, TransportError}
import io.parapet.net.transport.ServerTransport
import io.parapet.ProcessRef

import scala.concurrent.duration.*

import ServerProcess.*

final class ServerProcess[F[_]](
    transport: ServerTransport[F],
    sink: ProcessRef[Received | Failed],
    pollDelay: FiniteDuration = 10.millis,
    override val ref: ProcessRef[Reply] = ProcessRef[Reply]("net-server")
) extends Process[F, Reply, Nothing]:

  import dsl.*

  override val name: String = "net-server"

  private def receiveLoop: Program = flow {
    suspend(transport.receive).flatMap {
      case ReceiveResult.Received(RoutedMessage(routingId, message)) =>
        Received(routingId, message) ~> sink

      case ReceiveResult.Idle =>
        delay(pollDelay)

      case ReceiveResult.Failed(error) =>
        Failed(error) ~> sink
    } ++ receiveLoop
  }

  override def handle: Receive = {
    case Start =>
      fork(receiveLoop).void

    case Reply(routingId, message) =>
      suspend(transport.reply(routingId, message)).flatMap {
        case Right(_)    => unit
        case Left(error) => Failed(error) ~> sink
      }

    case Stop =>
      suspend(transport.close)
  }

object ServerProcess:
  final case class Reply(routingId: RoutingId, message: Message)    extends Event
  final case class Received(routingId: RoutingId, message: Message) extends Event
  final case class Failed(error: TransportError)                    extends Event
