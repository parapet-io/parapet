package io.parapet.core.processes

import cats.effect.Concurrent
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.Logger
import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.{Marchall, Start}
import io.parapet.core.processes.BullyLeaderElection._
import io.parapet.core.processes.PeerProcess.{CmdEvent, Send}
import io.parapet.core.{Channel, Event, Process, ProcessRef}
import io.parapet.p2p.Protocol
import io.parapet.syntax.logger.MDCFields
import org.slf4j.LoggerFactory
import io.parapet.syntax.logger._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import io.parapet.protobuf.bully.BullyLe

import scala.util.{Failure, Success}

/**
  * Bully leader election algorithm.
  * Main idea: each node (process) has a unique numeric id, a node with a highest id becomes a leader.
  *
  * Election process:
  * Wait until the number of nodes >= quorum size then start election.
  *
  * When a process P recovers from failure, or the failure detector indicates that the current coordinator has failed, P performs the following actions:
  *
  * If P has the highest process ID, it sends a Victory message to all other processes and becomes the new Coordinator. Otherwise, P broadcasts an Election message to all other processes with higher process IDs than itself.
  * If P receives no Answer after sending an Election message, then it broadcasts a Victory message to all other processes and becomes the Coordinator.
  * If P receives an Answer from a process with a higher ID, it sends no further messages for this election and waits for a Victory message. (If there is no Victory message after a period of time, it restarts the process at the beginning.)
  * If P receives an Election message from another process with a lower ID it sends an Answer message back and starts the election process at the beginning, by sending an Election message to higher-numbered processes.
  * If P receives a Coordinator message, it treats the sender as the coordinator.
  *
  * Message complexity: N^2
  **/
class BullyLeaderElection[F[_] : Concurrent](
                                              clientProcess: ProcessRef,
                                              peerProcess: ProcessRef,
                                              config: Config, hasher: String => Long) extends Process[F] {

  import BullyLeaderElection._
  import dsl._

  private[parapet] var _leader: Peer = _
  private var me: Peer = _

  private val answerDelay = 5.seconds
  private val coordinatorDelay = 5.seconds

  private[parapet] val peers = new java.util.TreeMap[Long, Peer]

  private var _state: State = Ready

  private var pendingReq: ProcessRef = _

  private val ch = new Channel[F]()

  private val logger = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))

  def switchToWaitForAnswer: DslF[F, Unit] = {
    eval(_state = WaitForAnswer) ++ switch(waitForAnswer)
  }

  def switchToWaitForCoordinator: DslF[F, Unit] = {
    eval(_state = WaitForCoordinator) ++ switch(waitingForCoordinator)
  }

  def switchToReady: DslF[F, Unit] = {
    eval(_state = Ready) ++ switch(waitForPeers)
  }

  def addPeer(peerId: String): DslF[F, Unit] = eval {
    val hash = hasher(peerId)
    peers.put(hash, Peer(peerId, hash))
  }

  def state: State = _state

  def leader: Option[Peer] = Option(_leader)

  def removePeer(peerId: String, cl: Peer => DslF[F, Unit] = _ => unit): DslF[F, Unit] = evalWith {
    val hash = hasher(peerId)
    peers.remove(hash)
  }(cl)


  def handleEcho: Receive = {
    case Echo => withSender(Echo ~> _)
  }

  def createMdc(e: Event): MDCFields =
    Map(
      "me" -> me,
      "state" -> state,
      "event" -> e,
      "ts" -> System.nanoTime()
    )

  def waitForPeers: Receive = handleEcho.orElse {
    case e@PeerProcess.CmdEvent(cmd) if cmd.getCmdType == Protocol.CmdType.JOINED =>
      val mdcFields = createMdc(e)
      eval(logger.mdc(mdcFields) { _ =>
        logger.debug("received JOINED")
      }) ++ addPeer(cmd.getPeerId) ++ startElection(mdcFields)
    case e@PeerProcess.CmdEvent(cmd) if cmd.getCmdType == Protocol.CmdType.LEFT =>
      val mdcFields = createMdc(e)
      eval(logger.mdc(mdcFields) { _ =>
        logger.debug("received LEFT")
      }) ++ removePeer(cmd.getPeerId, peer => {
        if (!fullQuorum) {
          eval {
            logger.mdc(mdcFields) { _ =>
              logger.debug(s"not enough processes in the cluster. discard the leader ${_leader}")
            }
            _leader = null // wait for new nodes
          }
        } else if (_leader == peer) {
          eval(
            logger.mdc(mdcFields) { _ =>
              logger.debug(s"the leader ${_leader} has gone. start election")
            }) ++ startElection(mdcFields)
        } else {
          unit
        }
      })

    case e@Command(Election(id)) =>
      eval(logger.mdc(createMdc(e)) { _ =>
        logger.debug("received Election")
      }) ++ Send(peers.get(id).uuid, Answer(me.id)) ~> peerProcess
    case e@Command(Answer(_)) => eval(logger.mdc(createMdc(e)) { _ =>
      logger.debug("ignore Answer")
    })

    case AnswerTimeout => unit // ignore
    case e@Command(Coordinator(id)) => eval {
      val mdcFields = createMdc(e)
      logger.mdc(mdcFields) { _ =>
        logger.debug("received Coordinator")
      }
      if (id > _leader.id) {
        logger.mdc(mdcFields) { _ =>
          logger.debug(s"set new leader: ${peers.get(id)}, old leader: ${_leader}")
        }
        _leader = peers.get(id)
      }
    }

    case r@Req(data) => flow {
      if (_leader == null) {
        withSender(s => Rep(Error, "leader is null".getBytes()) ~> s)
      } else if (_leader == me) {
        withSender(s => ch.send(r, clientProcess, {
          case Success(rep: Rep) => rep ~> s
          case Failure(e) => Rep(Error, Option(e.getMessage).getOrElse("").getBytes()) ~> s
        }))
      } else {
        withSender(s =>
          eval(require(pendingReq == null)) ++
            eval(pendingReq = s) ++ Send(_leader.uuid, ReqWithId(data, me.id)) ~> peerProcess) // forward to leader
      }
    }
    case Command(ReqWithId(data, peerId)) => flow {
      if (_leader == null) {
        Send(peers.get(peerId).uuid, Rep(Error, "leader is null".getBytes())) ~> peerProcess
      } else if (_leader == me) {
        ch.send(Req(data), clientProcess, {
          case Success(rep: Rep) => Send(peers.get(peerId).uuid, rep) ~> peerProcess
          case Failure(e) => Send(peers.get(peerId).uuid, Rep(Error, Option(e.getMessage).getOrElse("").getBytes())) ~> peerProcess
        })
      } else {
        Send(_leader.uuid, ReqWithId(data, me.id)) ~> peerProcess // forward to leader
      }
    }
    case Command(r: Rep) => flow {
      require(pendingReq != null)
      r ~> pendingReq ++ eval(pendingReq = null)
    }
  }

  def waitForAnswer: Receive = handleEcho.orElse {
    case e@Command(Answer(_)) =>
      eval(logger.mdc(createMdc(e)) { _ =>
        logger.debug("received Election")
      }) ++ switchToWaitForCoordinator ++ fork(delay(coordinatorDelay, CoordinatorTimeout ~> ref))
    case e@AnswerTimeout =>
      val mdcFields = createMdc(e)
      eval(logger.mdc(mdcFields) { _ =>
        logger.debug("didn't receive Answer message. set itself as leader")
      }) ++ becomeLeader(mdcFields) ++ switchToReady
    case e@Command(Election(id)) =>
      eval(logger.mdc(createMdc(e)) { _ =>
        logger.debug("received Election")
      }) ++ Send(peers.get(id).uuid, Answer(me.id)) ~> peerProcess
    case e@Command(Coordinator(id)) =>
      eval {
        _leader = peers.get(id)
        logger.mdc(createMdc(e)) { _ =>
          logger.debug(s"received Coordinator. ${_leader} elected as leader")
        }
      } ++ switchToReady
    case PeerProcess.CmdEvent(cmd) if cmd.getCmdType == Protocol.CmdType.JOINED => addPeer(cmd.getPeerId)
    case PeerProcess.CmdEvent(cmd) if cmd.getCmdType == Protocol.CmdType.LEFT => removePeer(cmd.getPeerId)
  }

  def waitingForCoordinator: Receive = handleEcho.orElse {
    case e@Command(Coordinator(id)) =>
      eval {
        _leader = peers.get(id)
        logger.mdc(createMdc(e)) { _ =>
          logger.debug(s"received Coordinator. ${_leader} elected as leader")
        }
      } ++ switchToReady
    case e@CoordinatorTimeout =>
      val mdcFields = createMdc(e)
      eval(
        logger.mdc(mdcFields) { _ =>
          logger.debug("didn't receive Coordinator message. restart election")
        }) ++ startElection(mdcFields)
    case Command(Answer(_)) => unit // ignore
    case PeerProcess.CmdEvent(cmd) if cmd.getCmdType == Protocol.CmdType.JOINED => addPeer(cmd.getPeerId)
    case PeerProcess.CmdEvent(cmd) if cmd.getCmdType == Protocol.CmdType.LEFT => removePeer(cmd.getPeerId)
  }

  /**
    * If P has the highest process ID, it sends a Coordinator message to all other processes and becomes the new Coordinator.
    * Otherwise, P broadcasts an Election message to all other processes with higher process IDs than itself and waits for Answer.
    */
  def startElection(mdc: MDCFields): DslF[F, Unit] = {
    eval {
      logger.mdc(mdc) { _ =>
        logger.debug(s"start election. current leader: ${_leader}, peers: $peers")
      }
      _leader = null
    } ++
      flow {
        if (!fullQuorum) {
          eval(logger.mdc(mdc) { _ =>
            logger.debug("not enough peers to start election. waiting for more peers...")
          })
        } else {
          val neighbors = peers.tailMap(me.id, false)
          if (neighbors.isEmpty) {
            eval(logger.mdc(mdc) { _ =>
              logger.debug("I have the highest ID. becomes the new leader")
            }) ++ becomeLeader(mdc)
          } else {
            eval(logger.mdc(mdc) { _ => {
              logger.debug(s"send Election to ${neighbors.values()}")
            }
            }) ++
              neighbors.values().asScala.map(p =>
                Send(p.uuid, Election(me.id)) ~> peerProcess).fold(unit)(_ ++ _) ++
              switchToWaitForAnswer ++ fork(delay(answerDelay, AnswerTimeout ~> ref))
          }
        }
      }
  }

  override def handle: Receive = {
    case Start => register(ref, ch) ++ PeerProcess.Reg(ref) ~> peerProcess
    case PeerProcess.Ack(uuid) => eval {
      me = Peer(uuid, hasher(uuid))
      logger.info(s"peer created. uuid = $uuid")
    } ++ switchToReady
  }

  def becomeLeader(mdc: MDCFields): DslF[F, Unit] = {
    evalWith {
      _leader = me
      val lowerBound = peers.headMap(me.id, false).values().asScala
      logger.mdc(mdc) { _ =>
        logger.debug(s"I became a leader. sending COORDINATOR message to $lowerBound")
      }
      lowerBound
    }(_.map(p => Send(p.uuid, Coordinator(me.id)) ~> peerProcess).fold(unit)(_ ++ _))
  }

  def fullQuorum: Boolean = peers.size() + 1 >= config.quorumSize
}


object BullyLeaderElection {

  case class Peer(uuid: String, id: Long)

  /**
    * Config for Bully LE
    *
    * @param quorumSize N/2 + 1 nodes
    */
  case class Config(quorumSize: Int)

  // API
  sealed trait Timeout extends Event
  case object CoordinatorTimeout extends Timeout
  case object AnswerTimeout extends Timeout
  case object CoordinatorAckTimeout extends Timeout

  trait Status
  object Ok extends Status
  object Error extends Status

  // Client API
  case class Req(bytes: Array[Byte]) extends Event

  sealed trait Command extends Event with Marchall

  /**
    * Sent to announce election.
    */
  case class Election(id: Long) extends Command {
    override def marshall: Array[Byte] = election(id)
  }

  /**
    * Answer (Alive) Message: Responds to the Election message.
    */
  case class Answer(id: Long) extends Command {
    override def marshall: Array[Byte] = answer(id)
  }

  /**
    * Coordinator (Victory) Message: Sent by winner of the election to announce victory.
    */
  case class Coordinator(id: Long) extends Command {
    override def marshall: Array[Byte] = coordinator(id)
  }

  case class ReqWithId(data: Array[Byte], id: Long) extends Command {
    override def marshall: Array[Byte] = req(id, data)
  }

  case class Rep(status: Status, data: Array[Byte]) extends Command {
    override def marshall: Array[Byte] = rep(0, status, data)
  }

  object Command {
    def unapply(event: Event): Option[Command] = {
      event match {
        case CmdEvent(cmd) if cmd.getCmdType == Protocol.CmdType.DELIVER =>
          val bleCmd = BullyLe.Command.parseFrom(cmd.getData)
          bleCmd.getCmdType match {
            case BullyLe.CmdType.ELECTION => Some(Election(bleCmd.getPeerId))
            case BullyLe.CmdType.ANSWER => Some(Answer(bleCmd.getPeerId))
            case BullyLe.CmdType.COORDINATOR => Some(Coordinator(bleCmd.getPeerId))
            case BullyLe.CmdType.REQ => Some(ReqWithId(bleCmd.getData.toByteArray, bleCmd.getPeerId))
            case BullyLe.CmdType.REP =>
              val r = BullyLe.Rep.parseFrom(bleCmd.getData)
              val s = if (r.getOk) Ok else Error
              Some(Rep(s, r.getData.toByteArray))
            case _ => None
          }
        case _ => None
      }
    }
  }

  def election(id: Long): Array[Byte] = {
    BullyLe.Command.newBuilder().setCmdType(BullyLe.CmdType.ELECTION).setPeerId(id).build().toByteArray
  }

  def answer(id: Long): Array[Byte] = {
    BullyLe.Command.newBuilder().setCmdType(BullyLe.CmdType.ANSWER).setPeerId(id).build().toByteArray
  }

  def coordinator(id: Long): Array[Byte] = {
    BullyLe.Command.newBuilder().setCmdType(BullyLe.CmdType.COORDINATOR).setPeerId(id).build().toByteArray
  }

  // sends request to master process
  def req(id: Long, data: Array[Byte]): Array[Byte] = {
    BullyLe.Command.newBuilder().setCmdType(BullyLe.CmdType.REQ).setPeerId(id).setData(ByteString.copyFrom(data)).build().toByteArray
  }

  // sends request to master process
  def rep(id: Long, status: Status, data: Array[Byte]): Array[Byte] = {
    BullyLe.Command.newBuilder().setCmdType(BullyLe.CmdType.REP)
      .setPeerId(id)
      .setData(BullyLe.Rep.newBuilder().setOk(status == Ok).setData(ByteString.copyFrom(data)).build().toByteString)
      .build().toByteArray
  }

  sealed trait State
  // in Ready state a leader can be null b/c the election process has not started yet or it's gone
  case object Ready extends State
  case object WaitForAnswer extends State
  case object WaitForCoordinator extends State

  object Echo extends Event

}