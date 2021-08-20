package io.parapet.cluster.node
import cats.effect.Concurrent
import com.typesafe.scalalogging.Logger
import io.parapet.core.api.Event
import io.parapet.core.{Channel, Process, ProcessRef}
import io.parapet.core.processes.net.AsyncClient.{Recv, Rep => CliRep, Send => ClientSend}
import io.parapet.core.processes.net.AsyncServer.{Message, Send => ServerSend}
import io.parapet.core.api.Cmd.leaderElection.{Who, WhoRep, Rep => LeRep, Req => LeReq}
import io.parapet.core.api.Cmd.cluster.{GetNodeInfo, NodeInfo}
import io.parapet.core.api.Cmd
import io.parapet.core.Events._

import scala.collection.mutable
import io.parapet.cluster.node.NodeProcess._
import io.parapet.core.Dsl.DslF
import io.parapet.core.processes.net.AsyncClient

class NodeProcess[F[_]: Concurrent](config: Config, client: ProcessRef, server: ProcessRef) extends Process[F] {

  private val logger = Logger[NodeProcess[F]]
  private val peers = mutable.Map[String, ProcessRef]()
  import dsl._

  private val srvChan = new Channel[F](ref)

  private var _leader: ProcessRef = _

  private def leader: Option[ProcessRef] = Option(_leader)

  private var _servers: Map[String, ProcessRef] = Map.empty

  def initServers: DslF[F, Unit] = {
    flow {
      val clients = config.servers.map(address => AsyncClient[F](ProcessRef(address), config.id, address))
      clients.map(client => register(ref, client)).fold(unit)(_ ++ _) ++ eval {
        _servers = config.servers.zip(clients.map(_.ref)).toMap
      }
    }
  }

  def initLeader: DslF[F, Unit] = {
    _servers.values.map(srv => ClientSend(Who(config.id).toByteArray) ~> srv).fold(unit)(_ ++ _) ++
      _servers.values.map(srv => Recv ~> srv).fold(unit)(_ ++ _)
  }

  def sendToPeer(id: String, data: Array[Byte]): DslF[F, Unit] =
    if (peers.contains(id)) {
      ClientSend(data) ~> peers(id)
    } else {
      blocking {
        srvChan.send(
          ServerSend(config.id, LeReq(config.id, GetNodeInfo(config.id, id).toByteArray).toByteArray),
          server,
          {
            case scala.util.Success(Message(_, data)) =>
              Cmd(data) match {
                case NodeInfo(peerAddress, Cmd.cluster.Code.Ok) =>
                  val peerClient = AsyncClient[F](ProcessRef(id), config.id, peerAddress)
                  register(ref, peerClient) ++ eval {
                    peers += id -> peerClient
                    ClientSend(data) ~> peerClient
                  }
                case NodeInfo(_, Cmd.cluster.Code.NotFound) => eval(logger.error(s"peer with id=$id not found"))
                case NodeInfo(_, Cmd.cluster.Code.Error) => eval(logger.error(s"failed to get info for peer id = $id"))
                case event => eval(logger.error(s"unexpected event = $event"))
              }
            case scala.util.Failure(err) => eval(Left(err.toString))
          },
        )
      }

    }

  override def handle: Receive = {
    case Start => register(ref, srvChan)
    case Req(id, data) => sendToPeer(id, data)
    case LeRep(id, data) => Rep(id, data) ~> client
    case CliRep(data) => Cmd(data) match {
      case WhoRep(addr, leader) => if(leader) {
        eval{_leader = _servers(addr)}
      }else {
        eval(logger.debug(s"server[$addr] is not a leader"))
      }
      case _ => unit
    }
  }

}

object NodeProcess {

  case class Config(id: String, port: Int, servers: Seq[String])

  case class Req(nodeId: String, data: Array[Byte]) extends Event
  case class Rep(nodeId: String, data: Array[Byte]) extends Event

}
