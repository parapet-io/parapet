package io.parapet.net.processes

import io.parapet.core.Events.{Start, Stop}
import io.parapet.core.Process
import io.parapet.core.api.Cmd
import io.parapet.net.RequestResponseServer
import io.parapet.{ProcessRef, core}

class TcpServerProcess[F[_]](
    server: RequestResponseServer[F],
    sink: ProcessRef,
    override val ref: ProcessRef = ProcessRef("net-tcp-server")
) extends Process[F]:

  import dsl._

  override val name: String = "tcp-server"

  private def receiveLoop: Program = flow {
    blocking {
      suspend(server.receive).flatMap {
        case Some(frame) => Cmd.netServer.Message(frame.clientId, frame.payload) ~> sink
        case None        => unit
      }
    } ++ receiveLoop
  }

  override def handle: Receive =
    {
      case Start =>
        fork(receiveLoop).void

      case Cmd.netServer.Send(clientId, data) =>
        blocking {
          suspend(server.reply(clientId, data))
        }

      case Stop =>
        suspend(server.close)
    }
