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
    case Request(data) =>
      suspend(transport.request(Message(data))).flatMap {
        case Right(response) => reply(Response(response.parts))
        case Left(error)     => reply(Failed(error))
      }

    case Stop =>
      suspend(transport.close)
  }

object ClientProcess:
  final case class Request(data: Vector[Array[Byte]]) extends Event
  object Request:
    def single(data: Array[Byte]): Request =
      Request(Vector(data))

  final case class Response(data: Vector[Array[Byte]]) extends Event
  final case class Failed(error: TransportError)       extends Event
