package io.parapet.core.processes

import cats.effect.Concurrent
import com.typesafe.scalalogging.Logger
import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.{Marchall, Start}
import io.parapet.core.processes.BullyLeaderElection._
import io.parapet.core.processes.PeerProcess.{CmdEvent, Send}
import io.parapet.core.{Event, Process, ProcessRef}
import io.parapet.p2p.Protocol
import io.parapet.p2p.Protocol.CmdType
import io.parapet.syntax.logger.MDCFields
import org.slf4j.LoggerFactory
import io.parapet.syntax.logger._
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.matching.Regex

/**
  * Bully leader election algorithm.
  * Main idea: each node (process) has a unique numeric id, a node with highest id becomes a leader.
  *
  * Election process:
  * Wait until the number of nodes >= quorum size
  * START_ELECTION:
  * send ELECTION message to every node with higher id and wait for ANSWER message otherwise elect itself
  * if ANSWER message was received then wait for COORDINATOR message, otherwise elect itself
  * if COORDINATOR message was received then set leader from the message, otherwise repeat election process
  */
class BullyLeaderElection[F[_] : Concurrent](peerProcess: ProcessRef,
                                             config: Config, hasher: String => Long) extends Process[F] {

  import BullyLeaderElection._
  import dsl._

  private var _leader: Peer = _
  private var me: Peer = _

  private val answerDelay = 5.seconds
  private val coordinatorDelay = 5.seconds

  private val peers = new java.util.TreeMap[Long, Peer]

  private var _state: State = Ready

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
            _leader = null
            // wait for new nodes
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
    case Start => PeerProcess.Reg(ref) ~> peerProcess
    case PeerProcess.Ack(uuid) => eval {
      me = Peer(uuid, hasher(uuid))
      println(s"Me: $me")
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

  sealed trait Command extends Event with Marchall {
    val id: Long
  }

  /**
    * Sent to announce election.
    */
  case class Election(id: Long) extends Command {
    override def marshall: Array[Byte] = s"${CommandType.Election.name}|$id".getBytes()
  }

  /**
    * Answer (Alive) Message: Responds to the Election message.
    */
  case class Answer(id: Long) extends Command {
    override def marshall: Array[Byte] = s"${CommandType.Answer.name}|$id".getBytes
  }

  /**
    * Coordinator (Victory) Message: Sent by winner of the election to announce victory.
    */
  case class Coordinator(id: Long) extends Command {
    override def marshall: Array[Byte] = s"${CommandType.Coordinator.name}|$id".getBytes
  }

  object CommandType extends Enumeration {

    protected case class Val(name: String) extends super.Val

    val Election: Val = Val("ELECTION")
    val Answer: Val = Val("ANSWER")
    val Coordinator: Val = Val("COORDINATOR")
  }

  object Command {
    // regex patterns
    val electionPattern: Regex = s"${CommandType.Election.name}\\|(\\d*)".r
    val answerPattern: Regex = s"${CommandType.Answer.name}\\|(\\d*)".r
    val coordinatorPattern: Regex = s"${CommandType.Coordinator.name}\\|(\\d*)".r

    def unapply(event: Event): Option[Command] = {
      event match {
        case CmdEvent(cmd) if cmd.getCmdType == CmdType.DELIVER =>
          new String(cmd.getData.toByteArray) match {
            case electionPattern(id) => Some(Election(id.toLong))
            case answerPattern(id) => Some(Answer(id.toLong))
            case coordinatorPattern(id) => Some(Coordinator(id.toLong))
            case _ => None
          }
        case _ => None
      }
    }
  }


  sealed trait State
  // in Ready state a leader can be null b/c the election process has not started yet or it's gone
  case object Ready extends State
  case object WaitForAnswer extends State
  case object WaitForCoordinator extends State

  object Echo extends Event


}