package io.parapet.cluster

import cats.effect.{Concurrent, IO}
import com.typesafe.scalalogging.Logger
import io.parapet.cluster.ClusterProcess.Cluster
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events.Start
import io.parapet.core.api.Cmd
import io.parapet.core.api.Cmd.cluster._
import io.parapet.core.api.Cmd.leaderElection.{Rep, Req}
import io.parapet.core.processes.RouletteLeaderElection._
import io.parapet.core.processes.net.AsyncServer.{Send => ServerSend}
import io.parapet.core.{Channel, Process, ProcessRef}
import org.zeromq.ZMQ.Socket
import org.zeromq.{SocketType, ZContext}

import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ClusterProcess(leaderElection: ProcessRef)(implicit ctxShit: Concurrent[IO]) extends Process[IO] {

  import dsl._

  private val ch = new Channel[IO](ref)

  private val zmqCtx = new ZContext()
  private val logger = Logger[ClusterProcess]
  private val cluster = new Cluster(zmqCtx)

  private def processJoin(clientId: String, join: Join): DslF[IO, Unit] = {
    for {
      node <- eval(cluster.getOrCreate(join.nodeId, join.address))
      _ <- eval(cluster.join(join.group, node))
      _ <- ServerSend(clientId, Cmd.cluster.JoinResult(join.nodeId, Code.Ok).toByteArray) ~> leaderElection
    } yield ()
  }

  override def handle: Receive = {
    case Start => register(ref, ch)
    case Req(clientId, data) =>
      Cmd(data) match {
        case join: Join =>
          eval(logger.debug(s"received $join")) ++ processJoin(clientId, join)
        case GetNodeInfo(_, id:String) => cluster.getNode(id) match {
          case Some(node) => ServerSend(clientId, Cmd.cluster.NodeInfo(node.address, Code.Ok).toByteArray) ~> leaderElection
          case None => ServerSend(clientId, Cmd.cluster.NodeInfo("", Code.NotFound).toByteArray) ~> leaderElection
        }
//        case getNodeInfo: GetNodeInfo =>
//          eval(logger.debug(s"received $getNodeInfo")) ++
//            (cluster.getNode(getNodeInfo.senderId) match {
//              case Some(senderNode) =>
//                if (senderNode.clientId != clientId) {
//                  eval {
//                    logger.error(s"expected node clientId=${senderNode.clientId} but received $clientId")
//                    // todo send a response
//                  }
//                } else {
//                  cluster.getNode(getNodeInfo.id) match {
//                    case Some(node) =>
//                      ServerSend(clientId, NodeInfo(node.address, Code.Ok).toByteArray) ~> leaderElection
//                    case None =>
//                      ServerSend(clientId, NodeInfo("", Code.NotFound).toByteArray) ~> leaderElection
//                  }
//                }
//              case None =>
//                eval {
//                  logger.error(s"node id=${getNodeInfo.senderId} doesn't exist")
//                  // todo send a response
//                }
//            })

      }
  }


}

object ClusterProcess {

  class Cluster(zmqCtx: ZContext) {
    private val logger = Logger[Cluster]
    private val nodes = mutable.Map.empty[String, Node]
    private val groups = mutable.Map.empty[String, mutable.Set[String]]

    def getOrCreate(id: String, address: String): Node = {
      nodes.get(id) match {
        case Some(node) =>
          val currAddress = node.address
          if (node.reconnect(address)) {
            logger.debug(s"node[$id] address has been updated. old=$currAddress, new=$address")
          }
          node
        case None =>
          logger.debug(s"node id=$id, address=$address has been created")
          val socket = zmqCtx.createSocket(SocketType.DEALER)
          socket.connect("tcp://" + address)
          val node = new Node(id, address, socket)
          nodes += id -> node
          node
      }
    }

    def join(group: String, node: Node): Unit = {
      if (!groups.contains(group)) {
        groups += group -> mutable.Set.empty
        logger.debug(s"new group '$group' has been created")
      }
      if (groups(group).add(node.id)) {
        logger.debug(s"$node has joined group=$group")
      }
    }

    def node(id: String): Node = nodes(id)

    def getNode(id: String): Option[Node] = nodes.get(id)

    def remove(id: String): Unit = {
      nodes.remove(id) match {
        case Some(node) => node.close()
        case None => ()
      }
    }

  }

  class Node(
      val id: String,
      private var _address: String,
      val socket: Socket
  ) {

    def address:String = _address

//    def state(state: NodeState): Unit = _state = state
//
//    def state: NodeState = _state

    def send(data: Array[Byte]): Unit =
      socket.send(data)

    def reconnect(newAddress: String): Boolean = {
      if (_address != newAddress) {
        socket.disconnect(_address)
        socket.connect(newAddress)
        _address = newAddress
        true
      } else {
        false
      }
    }

    def close(): Unit = {
      try {
        socket.close()
      } catch {
        case e: Exception => ()
      }
    }

    override def toString: String = s"id=$id, address=$address"
  }

//  sealed trait NodeState
//  case object Joined extends NodeState
//  case object Failed extends NodeState

}
