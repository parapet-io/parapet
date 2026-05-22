package io.parapet.net.processes

import io.parapet.core.Events.Stop
import io.parapet.core.Process
import io.parapet.Event
import io.parapet.net.transport.{Message, TransportError}
import io.parapet.net.transport.ClientTransport
import io.parapet.ProcessRef

import ClientProcess.*

final class ClientProcess[F[_]](
    transport: ClientTransport[F],
    override val ref: ProcessRef[Request] = ProcessRef[Request]("net-client")
) extends Process[F, Request, Response | Failed]:

  import dsl.*

  override val name: String = "net-client"

  override def handle: Receive = {
    case Request(message) =>
      suspend(transport.request(message)).flatMap {
        case Right(response) => reply(Response(response))
        case Left(error)     => reply(Failed(error))
      }

    case Stop =>
      suspend(transport.close)
  }

object ClientProcess:
  final case class Request(message: Message)     extends Event
  final case class Response(message: Message)    extends Event
  final case class Failed(error: TransportError) extends Event
