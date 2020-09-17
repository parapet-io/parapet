package io.parapet.core.processes

import cats.effect.Concurrent
import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.{Marshall, Start, Stop}
import io.parapet.core.{Event, Process, ProcessRef}
import io.parapet.p2p.{Node, Protocol, Config => P2pConfig}

class PeerProcess[F[_] : Concurrent](node: Node) extends Process[F] {

  import PeerProcess._
  import dsl._

  private var client: ProcessRef = _

  def receive: DslF[F, Unit] = flow {
    def step = for {
      cmd <- eval(node.receive())
      _ <- CmdEvent(cmd) ~> client
    } yield ()

    step ++ receive
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

  case class Reg(ref: ProcessRef) extends Event

  case class Ack(id: String) extends Event

  case class Send(peerId: String, data: Marshall) extends Event

}