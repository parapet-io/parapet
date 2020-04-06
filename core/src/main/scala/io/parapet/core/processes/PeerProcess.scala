package io.parapet.core.processes

import cats.effect.Concurrent
import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.{Marshall, Start, Stop}
import io.parapet.core.{Event, Process, ProcessRef}
import io.parapet.p2p.{Node, Protocol, Config => P2pConfig}
import io.parapet.core.doc.Doc._
import PeerProcess._

@Spec(description = "", api = Api(
  in = Array(classOf[Reg], classOf[Send]),
  out = Array(classOf[Ack], classOf[CmdEvent]))
)
class PeerProcess[F[_] : Concurrent](node: Node) extends Process[F] {

  import dsl._

  private var client: ProcessRef = _

  def receive: DslF[F, Unit] = {
    flow {
      evalWith(node.receive()) { cmd => CmdEvent(cmd) ~> client } ++ receive
    }
  }

  def initialized: Receive = {
    case Send(peer, data) => eval(node.send(peer, data.marshall))
    case Stop => onStop
  }

  def uninitialized: Receive = {
    case Start => unit
    case Reg(cliRef) => eval {
      client = cliRef
    } ++ Ack(node.getId) ~> cliRef ++ switch(initialized) ++ fork(receive)
    case Stop => onStop
  }

  override def handle: Receive = uninitialized

  def onStop: DslF[F, Unit] = eval {
    node.stop()
  }
}


object PeerProcess {

  def apply[F[_] : Concurrent](
                                config: P2pConfig = P2pConfig.builder().multicastIp("230.0.0.0")
                                  .protocolVer(1)
                                  .multicastPort(4446).build()): PeerProcess[F] = {
    new PeerProcess(new Node(config))
  }

  case class CmdEvent(cmd: Protocol.Command) extends Event

  @Doc("subscribes to peer events")
  case class Reg(
                  @Doc("client process ref") ref: ProcessRef
                ) extends Event

  @Doc("sent in response to Reg")
  case class Ack(
                  @Doc("peer id") id: String
                ) extends Event

  @Doc("sends an event to the specified peer")
  case class Send(
                   @Doc("receiver (peer) id") peerId: String,
                   @Doc("data to send") data: Marshall
                 ) extends Event

}