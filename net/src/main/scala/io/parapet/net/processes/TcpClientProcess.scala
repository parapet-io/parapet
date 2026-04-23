package io.parapet.net.processes

import io.parapet.core.Events.Stop
import io.parapet.core.Process
import io.parapet.core.api.Cmd
import io.parapet.net.RequestResponseClient
import io.parapet.{ProcessRef, core}

class TcpClientProcess[F[_]](
    client: RequestResponseClient[F],
    override val ref: ProcessRef = ProcessRef("net-tcp-client")
) extends Process[F]:

  import dsl._

  override val name: String = "tcp-client"

  override def handle: Receive =
    {
      case Cmd.netClient.Send(data, replyTo) =>
        blocking {
          suspend(client.request(data)).flatMap {
            case Some(response) =>
              replyTo match
                case Some(target) => Cmd.netClient.Rep(Some(response)) ~> target
                case None         => unit
            case None =>
              replyTo match
                case Some(target) => Cmd.netClient.Rep(None) ~> target
                case None         => unit
          }
        }

      case Stop =>
        suspend(client.close)
    }
