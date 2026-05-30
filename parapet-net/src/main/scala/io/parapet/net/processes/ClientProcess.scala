package io.parapet.net.processes

import io.parapet.core.Events.Stop
import io.parapet.core.Process
import io.parapet.Event
import io.parapet.net.transport.{Message, TransportError}
import io.parapet.net.transport.ClientTransport
import io.parapet.ProcessRef

import java.util.UUID

import ClientProcess.*

final class ClientProcess[F[_]](
    transport: ClientTransport[F],
    override val ref: ProcessRef[Request] = ProcessRef[Request]("net-client")
) extends Process[F, Request, Response | Failed]:

  import dsl.*

  override val name: String = "net-client"

  override def handle: Receive = {
    case Request(data) =>
      val correlationId = UUID.randomUUID().toString
      suspend(transport.request(Message(correlationId, data))).flatMap {
        case Right(response) =>
          if response.correlationId == correlationId then reply(Response(response.payload))
          else reply(Failed(TransportError.ProtocolViolation("client response correlation id does not match request")))
        case Left(error) =>
          reply(Failed(error))
      }

    case Stop =>
      suspend(transport.close)
  }

object ClientProcess:
  final case class Request(data: Array[Byte])    extends Event
  final case class Response(data: Array[Byte])   extends Event
  final case class Failed(error: TransportError) extends Event
