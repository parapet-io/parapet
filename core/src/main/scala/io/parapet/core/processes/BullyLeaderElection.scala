package io.parapet.core.processes

import java.util.UUID

import cats.effect.Concurrent
import io.parapet.core.Event.Start
import io.parapet.core.{Process, ProcessRef}
import io.parapet.p2p.Protocol
import shapeless.PolyDefns.~>

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class BullyLeaderElection[F[_] : Concurrent](peerProcess: ProcessRef) extends Process[F] {

  import dsl._
  import BullyLeaderElection._

  private val peers = mutable.Set.empty[Peer]

  private var leader: Peer = _

  def initialized: Receive = {
    case PeerProcess.CmdEvent(cmd) => cmd.getCmdType match {
      case Protocol.CmdType.DELIVER =>
      case Protocol.CmdType.JOINED => eval {
        peers += Peer(cmd.getPeerId)
      }
      case Protocol.CmdType.LEFT => eval {
        peers -= Peer(cmd.getPeerId)
      }
    }
  }

  override def handle: Receive = {
    case Start => PeerProcess.Reg(ref) ~> peerProcess
    case PeerProcess.Ack => ~>
  }
}


object BullyLeaderElection {

  case class Peer(id: String) {
    val numId: Long = UUID.fromString(id).getMostSignificantBits & Long.MaxValue

    override def equals(that: Any): Boolean = that.asInstanceOf[Peer].id == id
  }

  // Text protocol Cmd|[field=value]
  // Commands
  sealed trait Command {
    def toMessage: String
  }

  case class Election(id: Long) extends Command {
    def toMessage = s"ELECTION|id=$id"
  }

  case object Ok extends Command {
    override def toMessage: String = "OK"
  }

  case class Coordinator(id: Long) extends Command {
    override def toMessage = s"COORDINATOR|id=$id"
  }


}