package io.parapet.net.processes

import io.parapet.core.Events.{Start, Stop}
import io.parapet.core.Process
import io.parapet.core.api.Cmd
import io.parapet.net.RequestResponseServer
import io.parapet.{ProcessRef, core}

/** Adapter [[Process]] exposing a [[RequestResponseServer]] as an event-driven endpoint.
  *
  * On [[io.parapet.core.Events.Start]] it forks a receive loop that polls the server and forwards every inbound frame
  * to `sink` as a [[Cmd.netServer.Message]]. Outbound [[Cmd.netServer.Send]] events are written back to the originating
  * client. The server is closed on [[io.parapet.core.Events.Stop]].
  *
  * @param server
  *   the underlying transport server.
  * @param sink
  *   process to route inbound messages to.
  * @param ref
  *   optional process address; defaults to the well-known `"net-tcp-server"`.
  */
class TcpServerProcess[F[_]](
    server: RequestResponseServer[F],
    sink: ProcessRef[Cmd.netServer.Message],
    override val ref: ProcessRef[Cmd.netServer.Send] = ProcessRef[Cmd.netServer.Send]("net-tcp-server")
) extends Process[F, Cmd.netServer.Send]:

  import dsl._

  override val name: String = "tcp-server"

  private def receiveLoop: Program = flow {
    offload {
      suspend(server.receive).flatMap {
        case Some(frame) => Cmd.netServer.Message(frame.clientId, frame.payload) ~> sink
        case None        => unit
      }
    } ++ receiveLoop
  }

  override def handle: Receive = {
    case Start =>
      fork(receiveLoop).void

    case Cmd.netServer.Send(clientId, data) =>
      offload {
        suspend(server.reply(clientId, data))
      }

    case Stop =>
      suspend(server.close)
  }
