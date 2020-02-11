package io.parapet.core.processes

import cats.effect.Concurrent
import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.Marchall
import io.parapet.core.{Event, Process, ProcessRef}
import io.parapet.p2p.{Node, Protocol, Config => P2pConfig}

class PeerProcess[F[_] : Concurrent](node: Node) extends Process[F] {

  import PeerProcess._
  import dsl._

  private var client: ProcessRef = _

  def receive: DslF[F, Unit] = {
    flow {
      evalWith(node.receive()) { cmd => CmdEvent(cmd) ~> client } ++ receive
    }
  }

  def initialized: Receive = {
    case e => eval {
      e match {
        case marchall: Marchall =>
          node.send(marchall.marshall)
        case _ => println(s"event $e doesn't implement Marchall")
      }

    }
  }

  def uninitialized: Receive = {
    case Reg(cRef) => eval {
      client = cRef
    } ++ Ack ~> cRef ++ fork(receive) ++ switch(initialized)
    case _ => unit
  }

  override def handle: Receive = uninitialized
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

  case object Ack extends Event

}