package io.parapet.cluster

import cats.effect.{Concurrent, IO}
import com.typesafe.scalalogging.Logger
import io.parapet.ProcessRef
import io.parapet.cluster.ClusterProcess.Cluster
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events.Start
import io.parapet.core.api.Cmd
import io.parapet.core.api.Cmd.cluster._
import io.parapet.core.api.Cmd.leaderElection.{LeaderUpdate, Req}
import io.parapet.core.api.Cmd.netServer
import io.parapet.core.{Channel, Process}
import io.parapet.net.Node
import org.zeromq.{SocketType, ZContext}

import scala.collection.mutable

class ClusterProcess(override val ref: ProcessRef,
                     leaderElection: ProcessRef)(implicit ctxShit: Concurrent[IO]) extends Process[IO] {

  import dsl._

  private val ch = new Channel[IO](ref)

  private val zmqCtx = new ZContext()
  private val logger = Logger[ClusterProcess]
  private val cluster = new Cluster(zmqCtx)

  private def processJoin(clientId: String, join: Join): DslF[IO, Unit] = {
    for {
      node <- eval(cluster.getOrCreate(join.nodeId, join.address))
      _ <- eval(cluster.join(join.group, node))
      _ <- netServer.Send(clientId, Cmd.cluster.JoinResult(join.nodeId, Code.Ok).toByteArray) ~> leaderElection
      _ <- eval {
        val msg = Cmd.cluster.NodeInfo(node.id, node.address, Cmd.cluster.Code.Joined)
        cluster.shout(msg.toByteArray, cluster.groups(node.id))
      }
    } yield ()
  }

  override def handle: Receive = {
    case Start => register(ref, ch)
    case Req(clientId, data) =>
      Cmd(data) match {
        case join: Join =>
          eval(logger.debug(s"received $join")) ++ processJoin(clientId, join)
        case GetNodeInfo(_, id: String) => cluster.getNode(id) match {
          case Some(node) => netServer.Send(clientId, Cmd.cluster.NodeInfo(id, node.address, Code.Ok).toByteArray) ~> leaderElection
          case None => netServer.Send(clientId, Cmd.cluster.NodeInfo(id, "", Code.NotFound).toByteArray) ~> leaderElection
        }
      }
    case LeaderUpdate(leaderAddress) =>
      eval(logger.debug(s"leader has been changed. leader addr: $leaderAddress")) ++
        eval(cluster.shout(Cmd.leaderElection.LeaderUpdate(leaderAddress).toByteArray))
  }

}

object ClusterProcess {

  class Cluster(zmqCtx: ZContext) {
    private val logger = Logger[Cluster]
    private val _nodes = mutable.Map.empty[String, Node]
    private val _groupToNodes = mutable.Map.empty[String, mutable.Set[String]]
    private val _nodeToGroups = mutable.Map.empty[String, mutable.Set[String]]

    def getOrCreate(id: String, address: String): Node = {
      _nodes.get(id) match {
        case Some(node) =>
          val currAddress = node.address
          if (node.reconnect(address).get) {
            logger.debug(s"node[$id] address has been updated. old=$currAddress, new=$address")
          }
          node
        case None =>
          logger.debug(s"node id=$id, address=$address has been created")
          val socket = zmqCtx.createSocket(SocketType.DEALER)
          socket.connect("tcp://" + address)
          val node = new Node(id, address, socket)
          _nodes += id -> node
          node
      }
    }

    def join(group: String, node: Node): Unit = {
      if (!_groupToNodes.contains(group)) {
        _groupToNodes += group -> mutable.Set.empty
        logger.debug(s"new group '$group' has been created")
      }
      if (_groupToNodes(group).add(node.id)) {
        logger.debug(s"$node has joined group=$group")
      }
      _nodeToGroups.getOrElseUpdate(node.id, mutable.Set.empty) += group
      logger.debug(s"groups: ${_nodeToGroups}")
    }

    def node(id: String): Node = _nodes(id)

    def groups(nodeId: String): Set[String] = _nodeToGroups.getOrElse(nodeId, Set.empty).toSet

    def getNode(id: String): Option[Node] = _nodes.get(id)

    def remove(id: String): Unit = {
      _nodes.remove(id) match {
        case Some(node) => node.close()
        case None => ()
      }
    }

    def shout(data: Array[Byte], groups: Set[String] = Set.empty): Unit = {
      logger.debug(s"send data to groups: $groups")
      if (groups.isEmpty) {
        _nodes.values.foreach(node => node.send(data))
      } else {
        groups.flatMap(group => _groupToNodes.getOrElse(group, Set.empty)).foreach(id => {
          val n = node(id)
          logger.debug(s"send data to $n")
          n.send(data)
        })
      }
    }

  }

}
