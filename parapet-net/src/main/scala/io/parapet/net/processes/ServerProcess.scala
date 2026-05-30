package io.parapet.net.processes

import io.parapet.core.Events.{Start, Stop}
import io.parapet.core.Process
import io.parapet.Event
import io.parapet.net.transport.{Message, ReceiveResult, RoutedMessage, RoutingId, TransportError}
import io.parapet.net.transport.ServerTransport
import io.parapet.ProcessRef

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration.*
import ServerProcess.*
import io.parapet.net.transport.TransportError.ProtocolViolation

final class ServerProcess[F[_]](
    transport: ServerTransport[F],
    sink: ProcessRef[Received | Failed],
    pollDelay: FiniteDuration = 10.millis,
    override val ref: ProcessRef[Reply] = ProcessRef[Reply]("net-server")
) extends Process[F, Reply, Nothing]:

  import dsl.*

  override val name: String = "net-server"

  private val requests = new ConcurrentHashMap[String, RoutingId]()

  private def receiveLoop: Program = flow {
    suspend(transport.receive).flatMap {
      case ReceiveResult.Received(RoutedMessage(routingId, message)) =>
        eval {
          requests.put(message.correlationId, routingId)
          message.correlationId
        }.flatMap(id => Received(id, message.payload) ~> sink)

      case ReceiveResult.Idle =>
        delay(pollDelay)

      case ReceiveResult.Failed(error) =>
        Failed(error) ~> sink
    } ++ receiveLoop
  }

  override def handle: Receive = {
    case Start =>
      fork(receiveLoop).void

    case Reply(correlationId, data) =>
      dsl.unsafe.withSender { sender =>
        Option(requests.remove(correlationId)) match {
          case Some(routingId) =>
            suspend(transport.reply(routingId, Message(correlationId, data)))
              .flatMap {
                case Right(_)    => unit
                case Left(error) => Failed(error) ~> sender
              }
          case None =>
            Failed(ProtocolViolation(s"unknown correlationId: $correlationId")) ~> sender
        }
      }
    case Stop =>
      suspend(transport.close)
  }

object ServerProcess:
  final case class Reply(correlationId: String, data: Array[Byte])    extends Event
  final case class Received(correlationId: String, data: Array[Byte]) extends Event
  final case class Failed(error: TransportError)                      extends Event
