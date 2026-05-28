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

import java.util.UUID

final class ServerProcess[F[_]](
    transport: ServerTransport[F],
    sink: ProcessRef[Received | Failed],
    pollDelay: FiniteDuration = 10.millis,
    override val ref: ProcessRef[Reply] = ProcessRef[Reply]("net-server")
) extends Process[F, Reply, Nothing]:

  import dsl.*

  override val name: String = "net-server"

  private val requests = new ConcurrentHashMap[String, RouteInfo]()

  private def receiveLoop: Program = flow {
    suspend(transport.receive).flatMap {
      case ReceiveResult.Received(RoutedMessage(routingId, message)) =>
        eval {
          val correlationId = message.id.getOrElse(UUID.randomUUID().toString)
          requests.put(correlationId, RouteInfo(routingId, message.id))
          correlationId
        }.flatMap(id => Received(id, message.parts) ~> sink)

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
      Option(requests.remove(correlationId)) match {
        case Some(routeInfo) =>
          suspend(transport.reply(routeInfo.routingId, Message.single(data).copy(id = routeInfo.correlationId)))
            .flatMap {
              case Right(_)    => unit
              case Left(error) => Failed(error) ~> sink
            }
        case None => Failed(ProtocolViolation(s"unknown correlationId: $correlationId")) ~> sink
      }
    case Stop =>
      suspend(transport.close)
  }

object ServerProcess:
  private case class RouteInfo(routingId: RoutingId, correlationId: Option[String])

  final case class Reply(correlationId: String, data: Array[Byte])            extends Event
  final case class Received(correlationId: String, data: Vector[Array[Byte]]) extends Event
  final case class Failed(error: TransportError)                              extends Event
