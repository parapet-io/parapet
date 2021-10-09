package io.parapet.cluster.node
import cats.effect.Concurrent
import io.parapet.cluster.node.NodeProcess._
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events._
import io.parapet.core.api.Cmd.cluster
import io.parapet.core.api.Cmd.cluster._
import io.parapet.core.api.Cmd.leaderElection.{Who, WhoRep, Rep => LeRep, Req => LeReq}
import io.parapet.core.api.{Cmd, Event}
import io.parapet.core.processes.net.{AsyncClient, Node}
import io.parapet.core.processes.net.AsyncClient.{Rep => CliRep, Send => ClientSend}
import io.parapet.core.processes.net.AsyncServer.{Message, Send => ServerSend}
import io.parapet.core.{Channel, Process, ProcessRef}
import org.zeromq.{SocketType, ZContext}

import scala.concurrent.duration._
import scala.collection.mutable

class NodeProcess[F[_]: Concurrent](config: Config,
                                    client: ProcessRef,
                                    server: ProcessRef,
                                    override val ref: ProcessRef) extends Process[F] {

  //private val logger = Logger[NodeProcess[F]]
  private val peers = mutable.Map[String, Node]()
  import dsl._

  //private val srvChan = new Channel[F](ref)

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

    def step(attempts: Int): DslF[F, Unit] = {
      eval(println(s"getLeader. attempts made: $attempts")) ++
        sendSync(Who(config.id), _servers.values.toSeq, isLeader, 10.seconds).flatMap {
          case Some(WhoRep(address, _)) => eval {
            println(s"$address is leader")
            _leader = _servers(address)
          }
          case None => step(attempts + 1)
        }
    }

    step(0)
  }

  private def join(groupId: String): DslF[F, Unit] = {
    val req = LeReq(config.id, cluster.Join(config.id, s"${config.host}:${config.port}", groupId).toByteArray)
    val filter: Cmd => Boolean = {
      case cluster.JoinResult(_, _) => true
      case _ => false
    }

    def step(attempts: Int): DslF[F, Unit] = {
      eval(println(s"join. attempts made: $attempts")) ++
        sendSync(req, Seq(_leader), filter, 10.seconds).flatMap {
          case Some(cluster.JoinResult(_, cluster.Code.Ok)) => eval(println(s"node has joined cluster group: $groupId"))
          case Some(cluster.JoinResult(_, cluster.Code.Error)) => eval(println(s"node has failed to join cluster group: $groupId"))
          case None => step(attempts + 1)
        }
    }

    step(0)
  }

  private def getNodeInfo(id: String): DslF[F, Either[Throwable, Option[NodeInfo]]] = {
    // todo instead of using cond for timeout add timeout feature to channel
    val ch = Channel[F]
    val cond = new Cond[F](ch.ref, {
      case CliRep(data) => Cmd(data) match {
        case _: NodeInfo => true
        case _ => false
      }
      case _ => false
    }, 10.seconds)
    val data = LeReq(config.id, GetNodeInfo(config.id, id).toByteArray).toByteArray

    def release = halt(ch.ref) ++ halt(cond.ref)

    register(ref, ch) ++ register(ref, cond) ++
      ClientSend(data, Option(cond.ref)) ~> _leader ++
    ch.sendSync(Cond.Start, cond.ref) ++ ch.get.flatMap {
      case scala.util.Success(Cond.Result(Some(CliRep(data)))) =>
        Cmd(data) match {
          case n@NodeInfo(_, cluster.Code.Ok) => eval(Right(Option(n)).withLeft[Throwable])
          case NodeInfo(_, cluster.Code.NotFound) => eval(Right(Option.empty[NodeInfo]).withLeft[Throwable])
        }
     // case scala.util.Success(Cond.Result(None)) => eval(Left(new RuntimeException("request execution timeout")).withRight[Option[NodeInfo]])
      case scala.util.Failure(err) => eval(Left(err).withRight[Option[NodeInfo]])
    }.finalize(release)
  }

  private def getOrCreateNode(id: String): DslF[F, Option[Node]] = {
    peers.get(id) match {
      case Some(node) => eval(Option(node))
      case None => getNodeInfo(id).flatMap {
        case Left(err) => eval{
          println(err)
          Option.empty
        } // todo retry
        case Right(Some(NodeInfo(address, _))) => eval {
          val socket = zmqContext.createSocket(SocketType.DEALER)
          socket.setIdentity(id.getBytes())
          socket.connect(s"tcp://$address")
          val node = new Node(id, address, socket)
          println(s"node $node has been created")
          peers.put(id, node)
          Option(node)
        }
        case Right(None) => eval(Option.empty)
      }
    }
  }

  // should be used for m-m dialog where only one answer should be accepted
  private def sendSync[A >: Cmd](cmd: Cmd, servers: Seq[ProcessRef],
                         predicate: A => Boolean,
                         timeout: FiniteDuration): DslF[F, Option[A]] = {
    val ch = Channel[F]
    val cond = new Cond[F](ch.ref, {
      case CliRep(data) => predicate(Cmd(data))
      case _ => false
    }, timeout)
    val data = cmd.toByteArray
    def release = halt(ch.ref) ++ halt(cond.ref)

    register(ref, ch) ++ register(ref, cond) ++
      par(servers.map(srv => ClientSend(data, Option(cond.ref)) ~> srv): _*) ++
      ch.sendSync(Cond.Start, cond.ref) ++ ch.get.flatMap {
      case scala.util.Success(Cond.Result(Some(CliRep(data)))) => release ++ eval(Option(Cmd.apply(data)))
      case scala.util.Success(Cond.Result(None)) => release ++ eval(Option.empty)
      case scala.util.Failure(err) => release ++ eval(throw err)
    }
  }

  /*
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
                    peers += id -> peerClient.ref
                  } ++ ClientSend(data) ~> peerClient
                case NodeInfo(_, Cmd.cluster.Code.NotFound) => eval(println(s"peer with id=$id not found"))
                case NodeInfo(_, Cmd.cluster.Code.Error) => eval(println(s"failed to get info for peer id = $id"))
                case event => eval(println(s"unexpected event = $event"))
              }
            case scala.util.Failure(err) => eval(println(err.toString))
          },
        )
      }

    }
   */

  override def handle: Receive = {
    case Init => initServers ++ getLeader
    case NodeProcess.Join(group) => join(group)
    case NodeProcess.Req(id, data) => getOrCreateNode(id).flatMap{
      case Some(node) => eval(node.send(data))
      case None => eval(println(s"node[id=$id] not found"))
    }
    case Stop => eval(println("DONE"))
    // case Req(id, data) => sendToPeer(id, data)
    //   case LeRep(id, data) => Rep(id, data) ~> client
//    case CliRep(data) => Cmd(data) match {
//      case WhoRep(address, leader) => if(leader) {
//        eval{
//          println(s"leader = ${address}")
//          _leader = _servers(address)
//        } ++ ClientSend(LeReq(config.id,
//          Join(config.id, s"${config.host}:${config.port}", "").toByteArray).toByteArray) ~> _leader
//        // todo wait for JoinResult
//      }else {
//        eval(println(s"server[$address] is not a leader"))
//      }
//      case Ack(msg, code)  => eval(println(s"join result $msg $code"))
//      case JoinResult(_, code) => eval(println(s"join result = $code"))
//      case _ => unit
//    }
  }

}

object NodeProcess {

  case class Config(id: String, host: String, port: Int, servers: Seq[String])

  case object Init extends Event
  case class Join(group: String) extends Event
  case class Req(nodeId: String, data: Array[Byte]) extends Event
  case class Rep(nodeId: String, data: Array[Byte]) extends Event

}
