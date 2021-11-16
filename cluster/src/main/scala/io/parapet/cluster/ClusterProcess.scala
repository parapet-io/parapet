package io.parapet.cluster

import cats.effect.{Concurrent, IO}
import com.typesafe.scalalogging.Logger
import io.parapet.ProcessRef
import io.parapet.cluster.ClusterProcess.Cluster
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events.Start
import io.parapet.core.api.Cmd
import io.parapet.core.api.Cmd.{cluster => api}
import io.parapet.core.api.Cmd.leaderElection.{LeaderUpdate, Req, Broadcast}
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
  private var _stateVersion = 0L

  private def processJoin(clientId: String, join: api.Join): DslF[IO, Unit] = {
    for {
      node <- eval(cluster.getOrCreate(join.nodeId, join.address))
      _ <- eval(cluster.join(join.group, node))
      _ <- updateStateVersion
      _ <- publishStateUpdate
      _ <- netServer.Send(clientId, Cmd.cluster.JoinResult(join.nodeId, api.Code.Ok).toByteArray) ~> leaderElection
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
        case join: api.Join =>
          eval(logger.debug(s"received $join")) ++ processJoin(clientId, join)
        case api.GetNodeInfo(_, id: String) => cluster.getNode(id) match {
          case Some(node) => netServer.Send(clientId, Cmd.cluster.NodeInfo(id, node.address, api.Code.Ok).toByteArray) ~> leaderElection
          case None => netServer.Send(clientId, Cmd.cluster.NodeInfo(id, "", api.Code.NotFound).toByteArray) ~> leaderElection
        }
        case state: api.State => flow {
          if(state.version > _stateVersion) {
            eval {
              logger.debug(s"newer version of state has been received. old=${_stateVersion}, new=${state.version}")
              _stateVersion = state.version
              state.nodes.foreach { other=>
                val node = cluster.getOrCreate(other.id, other.address)
                node.reconnect(other.address)
                cluster.rejoinGroups(node.id, other.groups)
              }
            }
          } else {
            eval(logger.debug(s"ignore state update. current state version ${_stateVersion} > ${state.version}"))
          }
        }
      }
    case LeaderUpdate(leaderAddress) =>
      eval(logger.debug(s"leader has been changed. leader addr: $leaderAddress")) ++
        eval(cluster.shout(Cmd.leaderElection.LeaderUpdate(leaderAddress).toByteArray))
  }

  private def updateStateVersion: DslF[IO, Unit] = eval {
    _stateVersion = _stateVersion + 1
  }

  private def createStateMessage: api.State = {
    api.State(
      _stateVersion,
      cluster.nodes.map(node =>
        api.Node(
          id = node.id,
          protocol = node.protocol,
          address = node.address,
          groups = cluster.groups(node.id))))
  }

  private def publishStateUpdate: DslF[IO, Unit] = {
    for {
      state <- eval(createStateMessage)
      _ <- Broadcast(state.toByteArray) ~> leaderElection
    } yield ()
  }

}

object ClusterProcess {

  class Cluster(zmqCtx: ZContext) {
    private val logger = Logger[Cluster]
    private val _nodes = mutable.Map.empty[String, Node]
    private val _groupToNodes = mutable.Map.empty[String, Set[String]]
    private val _nodeToGroups = mutable.Map.empty[String, Set[String]]

    def nodes: List[Node] = _nodes.values.toList

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
      join(group, node.id)
    }

    private def join(group: String, nodeId: String): Unit = {
      _groupToNodes.updateWith(group) {
        case Some(nodes) =>
          if (!nodes.contains(nodeId)) {
            logger.debug(s"$nodeId has joined the group: $group")
          }
          Option(nodes + nodeId)
        case None =>
          logger.debug(s"new group '$group' has been created with node: $nodeId")
          Option(Set(nodeId))
      }
      _nodeToGroups.updateWith(nodeId) {
        case Some(groups) => Option(groups + group)
        case None => Option(Set(group))
      }
      logger.debug(s"$nodeId groups: ${_nodeToGroups(nodeId)}")
    }

    def rejoinGroups(nodeId: String, newGroups: Set[String]): Unit = {
      _nodeToGroups.getOrElse(nodeId, Set.empty).foreach { group =>
        _groupToNodes.updateWith(group) {
          case Some(nodes) => Option(nodes - nodeId)
          case None => Option.empty
        }
      }
      _nodeToGroups.put(nodeId, Set.empty)
      newGroups.foreach { group =>
        join(group = group, nodeId = nodeId)
      }
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
