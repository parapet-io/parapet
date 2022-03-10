package io.parapet.cluster.node

import cats.effect.Concurrent
import com.typesafe.scalalogging.Logger
import io.parapet.cluster.node.NodeProcess._
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events._
import io.parapet.core.api.Cmd
import io.parapet.core.api.Cmd.cluster._
import io.parapet.core.api.Cmd.leaderElection.{Who, WhoRep, Req => LeReq}
import io.parapet.core.api.Cmd.{cluster, netClient, netServer}
import io.parapet.core.{Channel, Cond, Process}
import io.parapet.net.{Address, AsyncClient, AsyncServer, ClientBroadcast, Node}
import io.parapet.{Event, ProcessRef}
import org.zeromq.{SocketType, ZContext}

import scala.collection.mutable
import scala.concurrent.duration._

class NodeProcess[F[_] : Concurrent](override val ref: ProcessRef,
                                     config: Config,
                                     client: ProcessRef,
                                     zmqContext: ZContext) extends Process[F] {

  import dsl._

  private val logger = Logger[NodeProcess[F]]
  private val peers = mutable.Map[String, Node]()
  private var _leader: ProcessRef = _
  // {host}:{port} -> netClientRef
  private var _servers: Map[String, ProcessRef] = Map.empty
  private val serverRef = ProcessRef(s"${config.id}-server")
  private val broadcast = ClientBroadcast[F]

  private def createServer: DslF[F, Unit] = flow {
    val srv = AsyncServer[F](ref = serverRef,
      zmqContext = zmqContext,
      address = Address.tcp("*", config.address.port),
      sink = ref)
    register(ref, srv)
  }

  private def initServers: DslF[F, Unit] =
    flow {
      val clients = config.servers.map(address => AsyncClient[F](ProcessRef(address.value),
        zmqContext, config.id, address))
      clients.map(client => register(ref, client)).fold(unit)(_ ++ _) ++ eval {
        _servers = config.servers.map(_.simple).zip(clients.map(_.ref)).toMap
      } ++ register(ref, broadcast)
    }

  private def getLeader: DslF[F, Unit] = {
    val isLeader: Cmd => Boolean = {
      case WhoRep(_, true) => true
      case _ => false
    }

    def step(attempts: Int): DslF[F, Unit] =
      eval(logger.debug(s"get leader. attempts made: $attempts")) ++
        sendSync(Who(config.id), _servers.values.toSeq, isLeader, 10.seconds).flatMap {
          case Some(WhoRep(address, _)) =>
            eval {
              logger.debug(s"$address is leader")
              _leader = _servers(address)
            }
          case None => step(attempts + 1)
        }

    step(0)
  }

  private def join(groupId: String): DslF[F, Unit] = {
    val req = LeReq(config.id, cluster.Join(config.id, config.address.simple, groupId).toByteArray)
    val filter: Cmd => Boolean = {
      case cluster.JoinResult(_, _) => true
      case _ => false
    }

    def step(attempts: Int): DslF[F, Unit] =
      eval(logger.debug(s"joining group '$groupId'. attempts made: $attempts")) ++
        sendSync(req, Seq(_leader), filter, 10.seconds).flatMap {
          case Some(cluster.JoinResult(_, cluster.Code.Ok)) =>
            eval(logger.debug(s"node has joined cluster group: $groupId"))
          case Some(cluster.JoinResult(_, cluster.Code.Error)) =>
            eval(logger.debug(s"node has failed to join cluster group: $groupId"))
          case None => step(attempts + 1)
        }

    step(0)
  }

  private def getNodeInfo(id: String): DslF[F, Either[Throwable, Option[NodeInfo]]] = {
    // todo instead of using cond for timeout add timeout feature to channel
    val ch = Channel[F]
    val cond = new Cond[F](
      {
        case netClient.Rep(data) => data.map(Cmd(_)).exists(_.isInstanceOf[NodeInfo])
        case _ => false
      },
      10.seconds)
    val data = LeReq(config.id, GetNodeInfo(config.id, id).toByteArray).toByteArray

    def release = halt(ch.ref) ++ halt(cond.ref)

    register(ref, ch) ++ register(ref, cond) ++
      netClient.Send(data, Option(cond.ref)) ~> _leader ++
      ch.send(Cond.Start, cond.ref)
        .flatMap {
          case scala.util.Success(Cond.Result(Some(netClient.Rep(Some(data))))) =>
            Cmd(data) match {
              case n@NodeInfo(_, _, cluster.Code.Ok) => eval(Right(Option(n)).withLeft[Throwable])
              case NodeInfo(_, _, cluster.Code.NotFound) => eval(Right(Option.empty[NodeInfo]).withLeft[Throwable])
            }
          case scala.util.Success(Cond.Result(Some(netClient.Rep(None)))) =>
            eval(Right(Option.empty[NodeInfo]).withLeft[Throwable])
          case scala.util.Success(Cond.Result(None)) =>
            eval(Right(Option.empty[NodeInfo]).withLeft[Throwable])
          case scala.util.Failure(err) => eval(Left(err).withRight[Option[NodeInfo]])
        }
        .guaranteed(release)
  }

  private def getOrCreateNode(id: String): DslF[F, Option[Node]] =
    peers.get(id) match {
      case Some(node) => eval(Option(node))
      case None =>
        getNodeInfo(id).flatMap {
          case Left(err) =>
            eval {
              logger.error(s"failed to obtain node[$id] info", err)
              Option.empty
            } // todo retry
          case Right(Some(NodeInfo(_, address, _))) =>
            eval {
              val socket = zmqContext.createSocket(SocketType.DEALER)
              socket.setIdentity(config.id.getBytes())
              socket.connect(s"tcp://$address")
              val node = new Node(id, address, socket)

              node.send(Cmd.cluster.Handshake.toByteArray)
              node.receive() match {
                case Some(value) =>
                  logger.debug(s"node $node has been created")
                  peers.put(id, node)
                  Option(node)
                case None =>
                  logger.warn(s"node id=$id is not responding")
                  Option.empty
              }

            }
          case Right(None) => eval(Option.empty)
        }
    }

  // should be used for 1-m dialog where only one answer should be accepted
  private def sendSync[A >: Cmd](cmd: Cmd,
                                 clusterNodes: Seq[ProcessRef],
                                 predicate: A => Boolean,
                                 timeout: FiniteDuration): DslF[F, Option[A]] = {
    val ch = Channel[F]
    val cond = new Cond[F](
      {
        case netClient.Rep(data) => data.exists(d => predicate(Cmd(d)))
        case _ => false
      },
      timeout)
    val data = cmd.toByteArray

    def release = halt(ch.ref) ++ halt(cond.ref)

    register(ref, ch) ++ register(ref, cond) ++
      par(clusterNodes.map(clusterNode => netClient.Send(data, Option(cond.ref)) ~> clusterNode): _*) ++
      ch.send(Cond.Start, cond.ref).flatMap {
        case scala.util.Success(Cond.Result(Some(netClient.Rep(Some(data))))) =>
          release ++ eval(Option(Cmd.apply(data)))
        case scala.util.Success(Cond.Result(Some(netClient.Rep(None)))) =>
          release ++ eval(Option.empty) // this code should be unreachable, unless cond logic is messed up
        case scala.util.Success(Cond.Result(None)) => release ++ eval(Option.empty)
        case scala.util.Failure(err) => release ++ raiseError(err)
      }
  }

  override def handle: Receive = {
    case Init => createServer ++ initServers ++ getLeader
    case NodeProcess.Send(data) => netClient.Send(LeReq(config.id, data).toByteArray) ~> _leader
    case NodeProcess.Join(group) => join(group)
    case NodeProcess.Req(id, data) =>
      getOrCreateNode(id).flatMap {
        case Some(node) =>
          eval(node.send(Cmd.clusterNode.Req(config.id, data).toByteArray))
        case None => eval(logger.debug(s"node id=$id not found"))
      }

    case netServer.Message(id, data) =>
      Cmd(data) match {
        case Cmd.clusterNode.Req(id, data) => NodeProcess.Req(id, data) ~> client
        case Cmd.leaderElection.LeaderUpdate(_, leaderAddr) =>
          eval(logger.debug(s"leader has been updated. old=${_leader} new=$leaderAddr")) ++ getLeader
        case Cmd.cluster.Handshake =>
          eval(logger.debug(s"received Handshake from $id")) ++
            Cmd.netServer.Send(id, Cmd.cluster.Ack("", Cmd.cluster.Code.HandshakeOk).toByteArray) ~> serverRef
        case Cmd.cluster.NodeInfo(id, addr, code) if code == Cmd.cluster.Code.Joined =>
          eval {
            peers.get(id) match {
              case Some(node) =>
                if (node.address != addr) {
                  logger.debug(s"$node address changed. new=$addr")
                  node.reconnect(addr)
                }
              case None => ()
            }
          }
        case Cmd.cluster.Leave(peerId) => eval(peers.remove(peerId)).flatMap {
          case Some(peer) => eval {
            logger.debug(s"peer: $peerId has left the cluster")
            peer.close()
          }
          case None => eval(logger.warn(s"cmd leave: peer with id: $peerId not found"))
        }
        case e => eval(logger.debug(s"unsupported cmd: $e"))
      }
    case Close =>
      val leave = Cmd.cluster.Leave(config.id).toByteArray
      eval(logger.info("node is closing")) ++
        eval(peers.values.foreach { peer =>
          try {
            peer.send(leave)
          } catch {
            case e: Exception => logger.error(s"failed to send leave to $peer", e)
          }
        }) ++ (for {
        bch <- eval(Channel[F])
        _ <- register(ref, bch)
        _ <- bch.send(ClientBroadcast.Send(Cmd.leaderElection.Req(config.id, leave).toByteArray,
          _servers.values.toList), broadcast.ref) // Response(List(Success(Rep(None)), Success(Rep(None))))
      } yield ())
  }

}

object NodeProcess {

  /**
    * A node process config
    *
    * @param id      a unique node id
    * @param address the node address
    * @param servers a list of cluster servers
    */
  case class Config(id: String, address: Address, servers: Seq[Address])

  case object Init extends Event

  case class Join(group: String) extends Event

  case class Send(data: Array[Byte]) extends Event

  case class Req(nodeId: String, data: Array[Byte]) extends Event

  case object Close extends Event

}
