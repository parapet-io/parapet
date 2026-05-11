package io.parapet.net.processes

import io.parapet.core.Events.Stop
import io.parapet.core.Process
import io.parapet.core.api.Cmd
import io.parapet.net.RequestResponseClient
import io.parapet.{ProcessRef, core}

/** Adapter [[Process]] exposing a [[RequestResponseClient]] as an event-driven endpoint.
  *
  * Accepts [[Cmd.netClient.Send]] commands; for each, forwards the payload over the underlying client and (if a
  * `replyTo` ref is given) routes the reply back as a [[Cmd.netClient.Rep]] event. Closes the client on
  * [[io.parapet.core.Events.Stop]].
  *
  * @param client
  *   the underlying transport client.
  * @param ref
  *   optional process address; defaults to the well-known `"net-tcp-client"`.
  */
class TcpClientProcess[F[_]](
    client: RequestResponseClient[F],
    override val ref: ProcessRef = ProcessRef("net-tcp-client")
) extends Process[F]:

  import dsl._

  override val name: String = "tcp-client"

  override def handle: Receive = {
    case Cmd.netClient.Send(data, replyTo) =>
      offload {
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
