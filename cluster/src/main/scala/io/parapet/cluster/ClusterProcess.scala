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

class ClusterProcess(leaderElection: ProcessRef)(implicit ctxShit: Concurrent[IO]) extends Process[IO] {

  import dsl._

  private val ch = new Channel[IO](ref)

  private val zmqCtx = new ZContext()
  private val logger = Logger[ClusterProcess]
  private val cluster = new Cluster(zmqCtx)

  override def handle: Receive = {
    case Start => register(ref, ch)
    case Req(clientId, data) =>
      Cmd(data) match {
        case join: Join =>
          ifLeader {
            cluster.getNode(join.nodeId) match {
              case Some(node) if node.address == join.address =>
                eval(logger.debug(s"node $node already joined")) ++
                  ServerSend(clientId, Ack("node has been added to the group", Code.Ok).toByteArray) ~> leaderElection
              case Some(node) =>
                eval {
                  logger.debug(s"nodes $node address has changed, rejoin. old=${node.address}, new=${join.address}")
                  cluster.remove(node.nodeId)
                } ++ broadcast(clientId, join)
              case None => eval(logger.debug("send broadcast to leader election process")) ++ broadcast(clientId, join)
            }
          } {
            eval(logger.debug(s"process join and send response to the leader id = $clientId")) ++
              processJoin(clientId, join) ++
              Rep(clientId, JoinResult(join.nodeId, Code.Ok).toByteArray) ~> leaderElection
          }
        case joinRes: JoinResult => eval(logger.debug(s"cluster received $joinRes"))
        case getNodeInfo: GetNodeInfo =>
          eval(logger.debug(s"received $getNodeInfo")) ++
            (cluster.getNode(getNodeInfo.senderId) match {
              case Some(senderNode) =>
                if (senderNode.clientId != clientId) {
                  eval {
                    logger.error(s"expected node clientId=${senderNode.clientId} but received $clientId")
                    // todo send a response
                  }
                } else {
                  cluster.getNode(getNodeInfo.id) match {
                    case Some(node) =>
                      ServerSend(clientId, NodeInfo(node.address, Code.Ok).toByteArray) ~> leaderElection
                    case None =>
                      ServerSend(clientId, NodeInfo("", Code.NotFound).toByteArray) ~> leaderElection
                  }
                }
              case None =>
                eval {
                  logger.error(s"node id=${getNodeInfo.senderId} doesn't exist")
                  // todo send a response
                }
            })

      }
  }

  // sends join to all nodes in the cluster and waits for responses
  private def broadcast(clientId: String, join: Join): DslF[IO, Unit] = {
    // Note: clientId != join.nodeId when Join sent by a leader election process
    val data = join.toByteArray
    blocking {
      ch.send(
        Broadcast(data),
        leaderElection,
        {
          case scala.util.Success(BroadcastResult(_)) =>
            // todo use reliable atomic broadcast
            // todo wait for acks from the majority of nodes
            // https://github.com/parapet-io/parapet/issues/47
            processJoin(clientId, join) ++
              eval(logger.debug(s"send Join result to $clientId")) ++
              ServerSend(clientId, Ack("node has been added to the group", Code.Ok).toByteArray) ~> leaderElection
          case scala.util.Failure(err) =>
            eval {
              logger.error("broadcast has failed", err)
            } ++ ServerSend(
              clientId,
              Ack(Option(err.getMessage).getOrElse(""), Code.Error).toByteArray,
            ) ~> leaderElection
        },
      )
    }
  }

  private def ifLeader(isLeader: => DslF[IO, Unit])(isNotLeader: => DslF[IO, Unit]): DslF[IO, Unit] =
    ch.send(
      IsLeader,
      leaderElection,
      { case scala.util.Success(IsLeaderRep(leader)) =>
        if (leader) eval(logger.debug("I'm a leader")) ++ isLeader
        else isNotLeader
        case scala.util.Success(_) => unit
        case scala.util.Failure(_) => unit
      },
    )

  private def processJoin(clientId: String, join: Join): DslF[IO, Unit] =
    eval {
      logger.debug(s"joining node id = ${join.nodeId}")
      val node = cluster.join(clientId, join)
      logger.debug(s"node $node created")
    }

}

object ClusterProcess {

  class Cluster(zmqCtx: ZContext) {
    private val logger = Logger[Cluster]
    private val nodes = new util.HashMap[String, Node]()
    private val nodesClientId = new util.HashMap[String, Node]()

    def join(clientId: String, join: Join): Node = {
      if (nodes.containsKey(join.nodeId)) {
        val node = nodes.remove(join.nodeId)
        node.socket.close()
        logger.debug(s"remove node id=${join.nodeId}")
      }
      logger.debug(s"add node id=${join.nodeId}")
      val socket = zmqCtx.createSocket(SocketType.DEALER)
      socket.connect("tcp://" + join.address)
      nodes.put(join.nodeId, new Node(clientId, join.nodeId, join.address, join.group, socket, Joined))
    }

    def node(id: String): Node = nodes.get(id)

    def getNode(id: String): Option[Node] =
      Option(nodes.get(id))

    def remove(id: String): Unit = {
      val node = nodes.remove(id)
      node.socket.close()
    }

  }

  class Node(
      val clientId: String,
      val nodeId: String,
      val address: String,
      val group: String,
      val socket: Socket,
      private var _state: NodeState,
  ) {

    def state(state: NodeState): Unit = _state = state

    def state: NodeState = _state

    def send(data: Array[Byte]): Unit =
      socket.send(data)

    override def toString: String = s"clientId=$clientId, nodeId=$nodeId, address=$address, group=$group"
  }

  sealed trait NodeState
  case object Joined extends NodeState
  case object Failed extends NodeState

}
