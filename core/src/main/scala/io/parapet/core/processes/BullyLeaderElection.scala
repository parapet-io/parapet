package io.parapet.core.processes

import cats.effect.Concurrent
import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.{Marchall, Start}
import io.parapet.core.processes.BullyLeaderElection._
import io.parapet.core.processes.PeerProcess.{CmdEvent, Send}
import io.parapet.core.{Event, Process, ProcessRef}
import io.parapet.p2p.Protocol
import io.parapet.p2p.Protocol.CmdType

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

  private var timestamp = System.nanoTime()

  private var _state: State = Ready


  // state machine
  def switchToWaitForAnswer: DslF[F, Unit] = {
    eval(_state = WaitForAnswer) ++ switch(waitForAnswer)
  }

  def switchToWaitForCoordinator: DslF[F, Unit] = {
    eval(_state = WaitForCoordinator) ++ switch(waitingForCoordinator) ++ eval(println("switched to waitingForCoordinator"))
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
    checkPeer(hash)
    peers.remove(hash)
  }(cl)

  def updateTimestamp: DslF[F, Unit] = eval(timestamp = System.nanoTime())

  def handleEcho: Receive = {
    case Echo => withSender(Echo ~> _)
  }

  def waitForPeers: Receive = handleEcho.orElse {
    case PeerProcess.CmdEvent(cmd) if cmd.getCmdType == Protocol.CmdType.JOINED =>
      addPeer(cmd.getPeerId) ++ startElection
    case PeerProcess.CmdEvent(cmd) if cmd.getCmdType == Protocol.CmdType.LEFT =>
      updateTimestamp ++
        eval(println(s"$me -- peerManagement - $timestamp - received $cmd")) ++
        removePeer(cmd.getPeerId, peer => {
          if (!fullQuorum) {
            eval {
              println(s"$me -- peerManagement - $timestamp - not enough processes in the cluster. discard the leader ${_leader}")
              _leader = null
              // wait for new nodes
            }
          } else if (_leader == peer) {
            eval(println(s"$me -- peerManagement - $timestamp - the leader ${_leader} has gone. start election")) ++ startElection
          } else {
            unit
          }
        })

    case Command(Election(id)) =>
      updateTimestamp ++
        eval {
          checkPeer(id)
          println(s"$me -- peerManagement - $timestamp - received Election from ${peers.get(id)}")
        } ++ Send(peers.get(id).uuid, Answer(me.id)) ~> peerProcess
    case Command(Answer(id)) => eval(println(s"$me -- peerManagement - $timestamp - ignore Answer from ${peers.get(id)}")) // ignore
    case AnswerTimeout => unit // ignore
    case Command(Coordinator(id)) => flow {
      if (_leader != null && _leader.id < id) {
        throw new IllegalStateException(s"$me -- peerManagement - received Coordinator from process with higher id. current leader ${_leader}, from message id=$id, ${peers.get(id)}")
      } else {
        unit
      }
    }
    case e =>
      updateTimestamp ++
        eval(println(s"$me -- peerManagement - $timestamp - unsupported event: $e"))
  }

  def waitForAnswer: Receive = handleEcho.orElse {
    case Command(Answer(id)) =>
      updateTimestamp ++
        eval(println(s"$me -- waitForAnswer - $timestamp - received Answer from ${peers.get(id)}")) ++
        switchToWaitForCoordinator ++
        fork(delay(coordinatorDelay, CoordinatorTimeout ~> ref))
    case AnswerTimeout =>
      updateTimestamp ++
        eval(println(s"$me -- waitForAnswer - $timestamp - didn't receive Answer message. set itself as leader")) ++
        becomeLeader ++ switchToReady
    case Command(Election(id)) =>
      updateTimestamp ++
        eval(checkPeer(id)) ++
        eval(println(s"$me -- waitForAnswer - $timestamp - received Election from ${peers.get(id)}")) ++
        Send(peers.get(id).uuid, Answer(me.id)) ~> peerProcess
    case Command(Coordinator(id)) =>
      updateTimestamp ++
        eval {
          checkPeer(id)
          println(s"$me -- waitForAnswer - $timestamp - received Coordinator from ${peers.get(id)}")
          _leader = peers.get(id)
        } ++ switchToReady
    case PeerProcess.CmdEvent(cmd) if cmd.getCmdType == Protocol.CmdType.JOINED => addPeer(cmd.getPeerId)
    case PeerProcess.CmdEvent(cmd) if cmd.getCmdType == Protocol.CmdType.LEFT => removePeer(cmd.getPeerId)
    case e =>
      updateTimestamp ++
        eval(println(s"$me -- waitForAnswer - $timestamp - unsupported event: $e"))
  }

  def waitingForCoordinator: Receive = handleEcho.orElse {
    case Command(Coordinator(id)) =>
      updateTimestamp ++
        eval {
          checkPeer(id)
          println(s"$me -- waitingForCoordinator - $timestamp - received Coordinator from ${peers.get(id)}")
          _leader = peers.get(id)
        } ++ switch(waitForPeers)
    case CoordinatorTimeout =>
      updateTimestamp ++
        eval(println(s"$me -- waitingForCoordinator - $timestamp - didn't receive Coordinator message. restart election")) ++
        startElection
    case Command(Answer(_)) => unit // ignore
    case PeerProcess.CmdEvent(cmd) if cmd.getCmdType == Protocol.CmdType.JOINED => addPeer(cmd.getPeerId)
    case PeerProcess.CmdEvent(cmd) if cmd.getCmdType == Protocol.CmdType.LEFT => removePeer(cmd.getPeerId)
    case e =>
      updateTimestamp ++
        eval(println(s"$me -- waitingForCoordinator - $timestamp - unsupported event: $e"))
  }

  /**
    * If P has the highest process ID, it sends a Coordinator message to all other processes and becomes the new Coordinator.
    * Otherwise, P broadcasts an Election message to all other processes with higher process IDs than itself and waits for Answer.
    */
  def startElection: DslF[F, Unit] = {
    updateTimestamp ++
      eval {
        println(s"$me -- startElection - $timestamp - old leader: ${_leader}. peers: $peers")
        _leader = null
      } ++
      flow {
        if (!fullQuorum) {
          eval(println(s"$me -- startElection - $timestamp - not enough nodes to start election. waiting for more peers..."))
        } else {
          val neighbors = peers.tailMap(me.id, false)
          if (neighbors.isEmpty) {
            // this process has the highest process ID
            becomeLeader
          } else {
            eval(s"$me -- startElection - $timestamp - send Election to ${neighbors.values()}") ++
              neighbors.values().asScala.map(p =>
                Send(p.uuid, Election(me.id)) ~> peerProcess ++
                  eval(println(s"$me -- startElection - $timestamp - sent Election to $p"))
              ).fold(unit)(_ ++ _) ++ switchToWaitForAnswer ++ fork(delay(answerDelay, AnswerTimeout ~> ref))
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

  def checkPeer(id: Long): Unit = {
    if (!peers.containsKey(id)) throw new IllegalStateException(s"peer with $id doesn't exist")
  }

  def becomeLeader: DslF[F, Unit] = {
    updateTimestamp ++
      evalWith {
        _leader = me
        val lowerBound = peers.headMap(me.id, false).values().asScala
        println(s"$me - $timestamp - I became a leader. sending COORDINATOR message to $lowerBound")
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