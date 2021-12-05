package io.parapet.cluster.node
import cats.effect.Concurrent
import com.typesafe.scalalogging.Logger
import io.parapet.cluster.node.NodeProcess._
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events._
import io.parapet.core.api.Cmd.cluster
import io.parapet.core.api.Cmd.cluster._
import io.parapet.core.api.Cmd.netClient
import io.parapet.core.api.Cmd.netServer
import io.parapet.core.api.Cmd.leaderElection.{Who, WhoRep, Rep => LeRep, Req => LeReq}
import io.parapet.core.api.Cmd
import io.parapet.net.{AsyncClient, Node}
import io.parapet.core.{Channel, Cond, Process}
import io.parapet.{Event, ProcessRef}
import org.zeromq.{SocketType, ZContext}

import scala.concurrent.duration._
import scala.collection.mutable

class NodeProcess[F[_]: Concurrent](
    config: Config,
    client: ProcessRef,
    server: ProcessRef,
    override val ref: ProcessRef
) extends Process[F] {
  import dsl._

  private val logger = Logger[NodeProcess[F]]
  private val peers = mutable.Map[String, Node]()
  private var _leader: ProcessRef = _
  private var _servers: Map[String, ProcessRef] = Map.empty

  private lazy val zmqContext = new ZContext(1)

  private def initServers: DslF[F, Unit] =
    flow {
      val clients = config.servers.map(address => AsyncClient[F](ProcessRef(address), config.id, "tcp://" + address))
      clients.map(client => register(ref, client)).fold(unit)(_ ++ _) ++ eval {
        _servers = config.servers.zip(clients.map(_.ref)).toMap
      }
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
    val req = LeReq(config.id, cluster.Join(config.id, s"${config.host}:${config.port}", groupId).toByteArray)
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
      ch.ref,
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
              case n @ NodeInfo(_, _, cluster.Code.Ok) => eval(Right(Option(n)).withLeft[Throwable])
              case NodeInfo(_, _, cluster.Code.NotFound) => eval(Right(Option.empty[NodeInfo]).withLeft[Throwable])
            }
          case scala.util.Success(Cond.Result(Some(netClient.Rep(None)))) =>
            eval(Right(Option.empty[NodeInfo]).withLeft[Throwable])
          case scala.util.Failure(err) => eval(Left(err).withRight[Option[NodeInfo]])
        }
        .finalize(release)
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
              logger.debug(s"node $node has been created")
              peers.put(id, node)
              Option(node)
            }
          case Right(None) => eval(Option.empty)
        }
    }

  // should be used for m-m dialog where only one answer should be accepted
  private def sendSync[A >: Cmd](
      cmd: Cmd,
      servers: Seq[ProcessRef],
      predicate: A => Boolean,
      timeout: FiniteDuration
  ): DslF[F, Option[A]] = {
    val ch = Channel[F]
    val cond = new Cond[F](
      ch.ref,
      {
        case netClient.Rep(data) => data.exists(d => predicate(Cmd(d)))
        case _ => false
      },
      timeout)
    val data = cmd.toByteArray
    def release = halt(ch.ref) ++ halt(cond.ref)

    register(ref, ch) ++ register(ref, cond) ++
      par(servers.map(srv => netClient.Send(data, Option(cond.ref)) ~> srv): _*) ++
      ch.send(Cond.Start, cond.ref).flatMap {
        case scala.util.Success(Cond.Result(Some(netClient.Rep(Some(data))))) => release ++ eval(Option(Cmd.apply(data)))
        case scala.util.Success(Cond.Result(Some(netClient.Rep(None)))) => release ++ eval(Option.empty)
        case scala.util.Success(Cond.Result(None)) => release ++ eval(Option.empty)
        case scala.util.Failure(err) => release ++ eval(throw err)
      }
  }

  override def handle: Receive = {
    case Init => initServers ++ getLeader
    case NodeProcess.Join(group) => join(group)
    case NodeProcess.Req(id, data) =>
      getOrCreateNode(id).flatMap {
        case Some(node) =>
          eval(logger.debug(s"node $node has found")) ++
            eval(node.send(Cmd.clusterNode.Req(config.id, data).toByteArray))
        case None => eval(logger.debug(s"node id=$id not found"))
      }

    case netServer.Message(id, data) =>
      Cmd(data) match {
        case Cmd.clusterNode.Req(id, data) => NodeProcess.Req(id, data) ~> client
        case Cmd.leaderElection.LeaderUpdate(_, leaderAddr) =>
          eval(logger.debug(s"leader has been updated. old=${_leader} new=$leaderAddr")) ++ getLeader
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
        case e => eval(logger.debug(s"unsupported cmd: $e"))
      }
    case Stop => eval(logger.debug("node is closed"))
  }

}

object NodeProcess {

  case class Config(id: String, host: String, port: Int, servers: Seq[String])

  case object Init extends Event
  case class Join(group: String) extends Event
  case class Req(nodeId: String, data: Array[Byte]) extends Event

}
