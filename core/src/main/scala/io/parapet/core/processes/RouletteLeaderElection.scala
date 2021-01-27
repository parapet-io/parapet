package io.parapet.core.processes

import java.nio.ByteBuffer
import com.typesafe.scalalogging.Logger
import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.Start
import io.parapet.core.processes.RouletteLeaderElection.ResponseCodes.AckCode
import io.parapet.core.processes.RouletteLeaderElection._
import io.parapet.core.processes.net.AsyncServer.Send
import io.parapet.core.utils.CorrelationId
import io.parapet.core.{Clock, Encoder, Event, ProcessRef}
import org.slf4j.MDC
import org.slf4j.event.Level

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Random

// Implementation of modified leader election algorithm https://arxiv.org/ftp/arxiv/papers/1703/1703.02247.pdf
class RouletteLeaderElection[F[_]](state: State) extends ProcessWithState[F, State](state) {

  import dsl._

  override val ref: ProcessRef = state.ref
  private val logger = Logger[RouletteLeaderElection[F]]

  override def handle: Receive = {
    case Start => heartbeatLoopAsync ++ monitorClusterLoopAsync

    // ------------------------BEGIN------------------------------ //
    case Begin =>
      implicit val correlationId: CorrelationId = CorrelationId()
      if (state.peers.hasMajority) {
        if (!state.voted) { // this process received a Propose message in the current round
          for {
            success <- eval(state.genRndNumAndUpdate())
            _ <-
              if (success) {
                eval {
                  state.votes = 1 // vote for itself
                } ++ sendPropose ++ waitForCoordinator
              } else {
                log(s"failed to generate a random num > threshold. the highest generated number: ${state.num}")
              }
          } yield ()
        } else {
          log("already voted in this round")
        }
      } else {
        log("cluster is not complete", Level.WARN)
      }

    // --------------------PROPOSE--------------------------- //
    case Propose(sender, num) =>
      implicit val correlationId: CorrelationId = CorrelationId()
      val peer = state.peers.get(sender)
      val action =
        if (state.coordinator) {
          Ack(state.addr, state.num, AckCode.COORDINATOR) ~> peer.netClient
        } else if (state.voted) {
          Ack(state.addr, state.num, AckCode.VOTED) ~> peer.netClient
        } else if (state.hasLeader) {
          Ack(state.addr, state.num, AckCode.ELECTED) ~> peer.netClient
        } else if (num > state.roundNum) {
          eval {
            state.voted = true
            state.roundNum = num
          } ++ Ack(state.addr, state.num, AckCode.OK) ~> peer.netClient ++ waitForHeartbeat
        } else {
          Ack(state.addr, state.num, AckCode.HIGH) ~> peer.netClient
        }

      log(s"received Propose($sender, $num)") ++ action

    // -----------------------ACK---------------------------- //
    case Ack(sender, num, code) =>
      implicit val correlationId: CorrelationId = CorrelationId()
      val action =
        code match {
          case AckCode.OK =>
            eval {
              state.votes = state.votes + 1
              state.peerNum += (sender -> num)
            } ++
              (if (!state.coordinator && receivedMajorityOfVotes) {
                 for {
                   _ <- eval(state.coordinator = true)
                   leaderAddr <- eval(state.roulette(state.peers.alive.map(_.addr)))
                   _ <- log("became coordinator")
                   _ <- Announce(state.addr) ~> state.peers.get(leaderAddr).netClient
                   _ <- log(s"send announce to $leaderAddr")
                   _ <- waitForHeartbeat
                 } yield ()
               } else {
                 unit
               })
          case AckCode.COORDINATOR => log(s"process '$sender' is already coordinator")
          case AckCode.VOTED => log(s"process '$sender' has already voted")
          case AckCode.HIGH => log(s"process '$sender' has the highest number")
          case AckCode.ELECTED => log("leader already elected")
        }

      log(s"received Ack(addr=$sender), num=$num, code=$code") ++ action

    // -----------------------ANNOUNCE---------------------------- //
    case Announce(sender) =>
      implicit val correlationId: CorrelationId = CorrelationId()
      log(s"received Announce(addr=$sender)") ++
        eval {
          require(state.leader.isEmpty, "current leader should be discarded")
          state.leader = Option(state.addr)
        }

    // -----------------------TIMEOUT(COORDINATOR)----------------------- //
    case Timeout(Coordinator) =>
      implicit val correlationId: CorrelationId = CorrelationId()
      if (!state.coordinator) { // todo: should it be wrapped in flow ?
        log("did not receive a majority of votes to become a coordinator") ++ reset
      } else {
        unit
      }

    // -----------------------TIMEOUT(LEADER)--------------------------------- //
    case Timeout(Leader) =>
      implicit val correlationId: CorrelationId = CorrelationId()
      if (state.leader.isEmpty) { // todo: should it be wrapped in flow ?
        log("leader hasn't been elected in the current round") ++ reset
      } else {
        unit
      }

    // --------------------HEARTBEAT------------------------------ //
    case Heartbeat(sender, leader) =>
      implicit val correlationId: CorrelationId = CorrelationId()
      val heartbeatFromLeader = leader.contains(sender)
      log(s"received Heartbeat(addr=$sender, leader=$leader)") ++
        eval(state.peers.get(sender).update()) ++ eval {
          if (heartbeatFromLeader) {
            logUnsafe("received heartbeat from a leader node")
          }
          leader match {
            case Some(leaderAddr) =>
              state.leader match {
                case None =>
                  if (state.clusterComplete && heartbeatFromLeader) {
                    state.leader = Option(leaderAddr)
                    logUnsafe(s"a new leader: '$leaderAddr' has been elected")
                  } else if (!state.clusterComplete && heartbeatFromLeader) {
                    logUnsafe(s"a new leader: '$leaderAddr' cannot be accepted b/c the cluster is not complete")
                  }
                case Some(currLeader) =>
                  if (currLeader != leaderAddr) {
                    val msg = s"received heartbeat from a new leader: '$leaderAddr', current: '$currLeader'; " +
                      "the current leader must be discarded"
                    logUnsafe(msg, Level.ERROR)
                    throw new IllegalStateException(msg)
                  } else {
                    logUnsafe(s"current leader: '$currLeader' is healthy")
                  }
              }
            case None =>
              state.leader match {
                case Some(currentLeader) if currentLeader == sender =>
                  logUnsafe(s"leader: '$sender' crashed and recovered")
                  resetUnsafe()
                case _ => () // heartbeat sent be a non leader node
              }
          }
        }

    // -----------------------WHO------------------------------- //
    case Who(clientId) =>
      withSender(sender => Send(clientId, state.leader.getOrElse("").getBytes()) ~> sender)
  }

  // -----------------------HELPERS------------------------------- //

  private def sendPropose: DslF[F, Unit] = {
    implicit val correlationId: CorrelationId = CorrelationId()
    state.peers.all
      .map { case (addr, peer) =>
        log(s"send propose to '$addr'") ++ Propose(state.addr, state.num) ~> peer.netClient
      }
      .fold(unit)(_ ++ _)
  }

  private def heartbeatLoopAsync: DslF[F, Unit] = fork(heartbeatLoop)

  private def heartbeatLoop: DslF[F, Unit] = {
    def step: DslF[F, Unit] = flow {
      sendHeartbeat ++ delay(state.delays.heartbeat) ++ step
    }

    step
  }

  private[core] def sendHeartbeat: DslF[F, Unit] = {
    implicit val correlationId: CorrelationId = CorrelationId()
    state.peers.all
      .map { case (addr, peer) =>
        log(s"send heartbeat to $addr") ++ Heartbeat(state.addr, state.leader) ~> peer.netClient
      }
      .fold(unit)(_ ++ _)
  }

  // This nodes waits for majority of votes to become a coordinator
  private def waitForCoordinator: DslF[F, Unit] = {
    implicit val correlationId: CorrelationId = CorrelationId()
    for {
      _ <- log("wait for majority of votes or heartbeat from a leader")
      _ <- fork(delay(state.timeouts.coordinator) ++ Timeout(Coordinator) ~> ref)
    } yield ()
  }

  private[core] def receivedMajorityOfVotes: Boolean = state.votes >= state.peers.size / 2 + 1

  private def waitForHeartbeat: DslF[F, Unit] = {
    implicit val correlationId: CorrelationId = CorrelationId()
    for {
      _ <- log("wait for heartbeat")
      _ <- fork(delay(state.timeouts.heartbeat) ++ Timeout(Leader) ~> ref)
    } yield ()
  }

  private[core] def monitorClusterLoopAsync: DslF[F, Unit] = fork(monitorClusterLoop)

  private[core] def monitorClusterLoop: DslF[F, Unit] = {
    def step: DslF[F, Unit] = flow {
      monitorCluster ++ delay(state.delays.monitor) ++ step
    }

    step
  }

  private[core] def monitorCluster: DslF[F, Unit] = {
    var action = eval(println("no action"))
    implicit val correlationId: CorrelationId = CorrelationId()
    try {
      val clusterComplete = state.peers.hasMajority
      action = if (clusterComplete && !state.hasLeader) {
        // we need to reset the state b/c the leader may have crashed
        log(s"cluster is complete but the leader is unknown / unreachable") ++ reset ++ Begin ~> ref
      } else if (!clusterComplete) {
        val msg = if (state.hasLeader) {
          "the leader is healthy"
        } else {
          "the leader is unknown / unreachable"
        }
        log(s"cluster is not complete. $msg") ++ reset
      } else {
        log("cluster is healthy")
      }
    } catch {
      case e: Exception =>
        action = log("unexpected error", Level.ERROR, Option(e))
    }

    log("monitor cluster") ++ action
  }

  def reset(implicit correlationId: CorrelationId): DslF[F, Unit] =
    eval(resetUnsafe())

  def resetUnsafe()(implicit correlationId: CorrelationId): Unit = {
    logUnsafe("reset the state and start a new election round")
    state.round = state.round + 1
    state.votes = 0
    state.num = 0.0
    state.roundNum = 0.0
    state.coordinator = false
    state.peerNum.clear()
    state.leader = Option.empty[String]
    state.voted = false
  }

  def log(msg: => String, lvl: Level = Level.DEBUG, ex: Option[Exception] = Option.empty)(implicit
      line: sourcecode.Line,
      file: sourcecode.File,
      correlationId: CorrelationId,
  ): DslF[F, Unit] =
    eval(logUnsafe(msg, lvl))

  private def logUnsafe(msg: => String, lvl: Level = Level.DEBUG, ex: Option[Exception] = Option.empty)(implicit
      line: sourcecode.Line,
      file: sourcecode.File,
      correlationId: CorrelationId,
  ): Unit = {
    MDC.put("line", line.value.toString)
    MDC.put("correlationId", correlationId.value)
    State.addToMDC(state)
    lvl match {
      case Level.ERROR =>
        ex match {
          case Some(cause) => logger.error(msg, cause)
          case None => logger.error(msg)
        }
      case Level.WARN => logger.warn(msg)
      case Level.INFO => logger.info(msg)
      case Level.DEBUG => logger.debug(msg)
      case Level.TRACE => logger.trace(msg)
    }
    MDC.clear()
  }

}

object RouletteLeaderElection {

  case class VoteNum(min: Double, max: Double)

  type Addr = String // network address w/o protocol, i.e. (ip|host):port . it's as a unique identifier of a process
  type NetClient = ProcessRef // a process that performs a network operations
  type RandomNum = Int => VoteNum

  case class Timeouts(
      coordinator: FiniteDuration = 60.seconds,
      heartbeat: FiniteDuration = 60.seconds, // wait for a Heartbeat h where h.addr == h.leader
  )

  case class Delays(
      election: FiniteDuration = 10.seconds,
      heartbeat: FiniteDuration = 5.seconds,
      monitor: FiniteDuration = 10.seconds,
  )

  class State(
      val ref: ProcessRef,
      val addr: Addr,
      val peers: Peers,
      val random: RandomNum = RandomNumGen,
      val roulette: Vector[Addr] => Addr = Roulette,
      val timeouts: Timeouts = Timeouts(),
      val delays: Delays = Delays(),
      val rndNumMinRounds: Int = GenNumAttempts,
      val threshold: Double = GenNumThreshold,
  ) {
    var num: Double = 0.0
    var roundNum: Double = 0.0
    var votes = 0
    var round = 0
    val peerNum: mutable.Map[String, Double] = mutable.Map.empty // peer addr to its random num
    var leader = Option.empty[Addr]
    // this flag indicates that this process was chosen as coordinator
    var coordinator = false
    // this flag indicates that this process has already voted in the current round
    var voted = false

    def hasLeader: Boolean = leader.exists(l => addr == l || peers.get(l).isAlive)

    def clusterComplete: Boolean = peers.hasMajority

    /** Generates a random number exactly [[rndNumMinRounds]] times and assigns a highest random number to [[num]].
      * Sets [[roundNum]] to [[VoteNum.max]] if [[VoteNum.min]] > [[threshold]]
      *
      * @return true if [[VoteNum.min]] > [[threshold]], otherwise false
      */
    def genRndNumAndUpdate(): Boolean = {
      val res = random(rndNumMinRounds)
      num = res.max
      if (res.min > threshold) {
        roundNum = res.max
      }
      res.min > threshold
    }
  }

  object State {
    private val logFields = Set(
      "round",
      "addr",
      "num",
      "roundNum",
      "leader",
      "votes",
      "coordinator",
      "peerNum",
      "voted",
    )

    def getLogValues(s: State): Map[String, String] =
      classOf[State].getDeclaredFields
        .filter(f => logFields.contains(f.getName))
        .map { field =>
          field.setAccessible(true)
          field.getName -> Option(field.get(s)).map(_.toString).getOrElse("null")
        }
        .toMap

    def addToMDC(s: State): Unit = {
      getLogValues(s).foreach { case (k, v) =>
        MDC.put(k, v)
      }
      MDC.put("peers", s.peers.info)
    }
  }

  val GenNumAttempts = 1
  val GenNumThreshold = 0.85

  val RandomNumGen: RandomNum = i => {
    val r = new Random()
    var min = 1.0
    var max = 0.0
    (0 until i).foreach { _ =>
      val n = r.nextDouble()
      min = Math.min(min, n)
      max = Math.max(max, n)
    }

    VoteNum(min, max)
  }

  val Roulette: Vector[Addr] => Addr = processes => {
    val r = new Random()
    processes(r.nextInt(processes.size))
  }

  // @formatter:off
  sealed trait Phase
  case object Coordinator extends Phase
  case object Leader extends Phase

  sealed trait API extends Event
  case object Begin extends API
  case class Propose(addr: Addr, num: Double) extends API
  case class Ack(addr: Addr, num: Double, code: AckCode) extends API
  case class Announce(addr: Addr) extends API
  case class Heartbeat(addr: Addr, leader: Option[Addr]) extends API
  case class Timeout(phase: Phase) extends API
  case class Who(clientId: String) extends API


  object ResponseCodes {

    sealed class AckCode(val value: Int)

    object AckCode {
      case object OK          extends AckCode(0)  // success
      case object COORDINATOR extends AckCode(1)  // error: the current process is coordinator
      case object VOTED       extends AckCode(2)  // error: the current process has already voted
      case object ELECTED     extends AckCode(3)  // error: leader already elected
      case object HIGH        extends AckCode(4)  // error: the current process has a higher number than sender

      def apply(value: Int): AckCode = value match {
        case 0 => OK
        case 1 => COORDINATOR
        case 2 => VOTED
        case 3 => ELECTED
        case 4 => HIGH
        case _ => throw new IllegalArgumentException(s"unsupported ack code $value")
      }
    }
  }
  // @formatter:on

  // Protocol format [tag: int32, body: byte[]]
  // Tags
  val PROPOSE_TAG = 1
  val ACK_TAG = 2
  val ANNOUNCE_TAG = 3
  val HEARTBEAT_TAG = 4
  val WHO_TAG = 5

  val encoder: Encoder = new Encoder {
    override def write(e: Event): Array[Byte] =
      e match {
        case Propose(addr, num) =>
          val addrBytes = addr.getBytes()
          ByteBuffer
            .allocate(4 + 4 + addrBytes.length + 8)
            .putInt(PROPOSE_TAG)
            .putInt(addrBytes.length)
            .put(addrBytes)
            .putDouble(num)
            .array()
        case Ack(addr, num, code) =>
          val addrBytes = addr.getBytes()
          ByteBuffer
            .allocate(4 + 4 + addrBytes.length + 8 + 4)
            .putInt(ACK_TAG)
            .putInt(addrBytes.length)
            .put(addrBytes)
            .putDouble(num)
            .putInt(code.value)
            .array()
        case Announce(addr) =>
          val addrBytes = addr.getBytes()
          ByteBuffer
            .allocate(4 + 4 + addrBytes.length)
            .putInt(ANNOUNCE_TAG)
            .putInt(addrBytes.length)
            .put(addrBytes)
            .array()
        case Heartbeat(addr, leader) =>
          val addrBytes = addr.getBytes()
          val leaderAddrBytes = leader.getOrElse("").getBytes()
          ByteBuffer
            .allocate(4 + (4 + addrBytes.length) + (4 + leaderAddrBytes.length))
            .putInt(HEARTBEAT_TAG)
            .putInt(addrBytes.length)
            .put(addrBytes)
            .putInt(leaderAddrBytes.length)
            .put(leaderAddrBytes)
            .array()
      }

    override def read(data: Array[Byte]): Event = {
      val buf = ByteBuffer.wrap(data)
      val tag = buf.getInt()
      tag match {
        case PROPOSE_TAG =>
          val addrBytes = new Array[Byte](buf.getInt())
          buf.get(addrBytes)
          val num = buf.getDouble
          Propose(new String(addrBytes), num)
        case ACK_TAG =>
          val addrBytes = new Array[Byte](buf.getInt())
          buf.get(addrBytes)
          val num = buf.getDouble
          val code = buf.getInt
          Ack(new String(addrBytes), num, AckCode(code))
        case ANNOUNCE_TAG =>
          val addrBytes = new Array[Byte](buf.getInt())
          buf.get(addrBytes)
          Announce(new String(addrBytes))
        case HEARTBEAT_TAG =>
          val addrBytes = new Array[Byte](buf.getInt())
          buf.get(addrBytes)
          val leaderAddrBytes = new Array[Byte](buf.getInt())
          buf.get(leaderAddrBytes)
          Heartbeat(new String(addrBytes), Option(new String(leaderAddrBytes)).filter(_.nonEmpty))
        case WHO_TAG =>
          val clientIdBytes = new Array[Byte](buf.getInt())
          buf.get(clientIdBytes)
          Who(new String(clientIdBytes))
      }
    }
  }

  def boolToShort(b: Boolean): Short =
    if (b) 1
    else 0

  def shortToBool(s: Short): Boolean = s == 1

  class Peer(
      val addr: Addr,
      val netClient: NetClient,
      val timeoutMs: Long,
      val clock: Clock,
  ) {
    private var lastPingAt: Long = 0

    def update(): Unit =
      update(clock.currentTimeMillis)

    private[core] def update(pingAt: Long): Unit =
      lastPingAt = pingAt

    def isAlive: Boolean = isAlive(clock.currentTimeMillis)

    private[core] def isAlive(cur: Long): Boolean =
      lastPingAt >= cur - timeoutMs

    override def toString: String =
      s"addr=$addr, netClientRef=$netClient"
  }

  case class Peers(peers: Vector[Peer]) {
    private val map: Map[Addr, Peer] = peers.map(p => p.addr -> p).toMap
    val netClients: Seq[NetClient] = peers.map(_.netClient)

    def all: Map[Addr, Peer] = map

    @throws[IllegalStateException]
    def get(addr: Addr): Peer = map.get(addr) match {
      case Some(value) => value
      case None => throw new IllegalStateException(s"peer with addr=$addr doesn't exist")
    }

    def getNetClient(addr: Addr): NetClient = get(addr).netClient

    def alive: Vector[Peer] = peers.filter(_.isAlive)

    def size: Int = peers.size

    def hasMajority: Boolean = {
      val liveNodes = alive.size + 1
      liveNodes >= (peers.size + 1) / 2 + 1
    }

    def info: String = map
      .map { case (_, v) =>
        val status = if (v.isAlive) "alive" else "unavailable"
        s"{peer=${v.addr}, status=$status}"
      }
      .mkString("; ")
  }

  object Peers {
    def apply(peers: Map[Addr, NetClient], timeoutMs: Long = 10000, clock: Clock = Clock()): Peers =
      new Peers(peers.map { case (peerAddr, netClient) =>
        new Peer(peerAddr, netClient, timeoutMs, clock)
      }.toVector)
  }

}
