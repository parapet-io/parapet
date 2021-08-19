package io.parapet.core.processes


import com.typesafe.scalalogging.Logger
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events.Start
import io.parapet.core.api.{Cmd, Event}
import io.parapet.core.api.Cmd.leaderElection._
import io.parapet.core.processes.RouletteLeaderElection._
import io.parapet.core.processes.net.AsyncClient.{Send => CliSend}
import io.parapet.core.processes.net.AsyncServer.{Message => SrvMessage, Send => SrvSend}
import io.parapet.core.utils.CorrelationId
import io.parapet.core.{Clock, ProcessRef}
import org.slf4j.MDC
import org.slf4j.event.Level

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Random

class RouletteLeaderElection[F[_]](state: State, sink: ProcessRef = ProcessRef.BlackHoleRef)
  extends ProcessWithState[F, State](state) {

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
          Ack(state.addr, state.num, AckCode.Coordinator).toClient ~> peer.netClient
        } else if (state.voted) {
          Ack(state.addr, state.num, AckCode.Voted).toClient ~> peer.netClient
        } else if (state.hasLeader) {
          Ack(state.addr, state.num, AckCode.Elected).toClient ~> peer.netClient
        } else if (num > state.roundNum) {
          eval {
            state.voted = true
            state.roundNum = num
          } ++ Ack(state.addr, state.num, AckCode.Ok).toClient  ~> peer.netClient ++ waitForHeartbeat
        } else {
          Ack(state.addr, state.num, AckCode.High).toClient  ~> peer.netClient
        }

      log(s"received Propose($sender, $num)") ++ action

    // -----------------------ACK---------------------------- //
    case Ack(sender, num, code) =>
      implicit val correlationId: CorrelationId = CorrelationId()
      val action =
        code match {
          case AckCode.Ok =>
            eval {
              state.votes = state.votes + 1
              state.peerNum += (sender -> num)
            } ++
              (if (!state.coordinator && receivedMajorityOfVotes) {
                 for {
                   _ <- eval(state.coordinator = true)
                   leaderAddr <- eval(state.roulette(state.peers.alive.map(_.address)))
                   _ <- log("became coordinator")
                   _ <- Announce(state.addr).toClient ~> state.peers.get(leaderAddr).netClient
                   _ <- log(s"send announce to $leaderAddr")
                   _ <- waitForHeartbeat
                 } yield ()
               } else {
                 unit
               })
          case AckCode.Coordinator => log(s"process '$sender' is already coordinator")
          case AckCode.Voted => log(s"process '$sender' has already voted")
          case AckCode.High => log(s"process '$sender' has the highest number")
          case AckCode.Elected => log("leader already elected")
        }

      log(s"received Ack(addr=$sender), num=$num, code=$code") ++ action

    // -----------------------ANNOUNCE---------------------------- //
    case Announce(sender) =>
      implicit val correlationId: CorrelationId = CorrelationId()
      log(s"received Announce(sender=$sender)") ++
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
      withSender(server => SrvSend(clientId,
        WhoRep(state.addr, state.leader.contains(state.addr)).toByteArray) ~> server)

    case IsLeader =>
      withSender(sender => IsLeaderRep(state.leader.contains(state.addr)) ~> sender)

    // -------------------- BROADCAST ----------------------------//
    case Broadcast(data) =>
      implicit val correlationId: CorrelationId = CorrelationId()
      val msg = CliSend(Req(state.id, data).toByteArray)
      log("received broadcast") ++
        state.peers.netClients.foldLeft(unit)((acc, client) => acc ++ msg ~> client) ++
        withSender(sender => BroadcastResult(state.peers.size / 2) ~> sender)

    case BroadcastResult(res) =>
      implicit val correlationId: CorrelationId = CorrelationId()
      log(s"received broadcast result: $res")

    // -----------------------REQ------------------------------- //
    case req: Req => req ~> sink

    // ----------------------- REP -------------------------------//
    // Rep is sent by sink process in event of Req
    case Rep(clientId, data) =>
      implicit val correlationId: CorrelationId = CorrelationId()
      if (state.peers.idExists(clientId)) {
        log(s"received Rep from clientId: $clientId, send reply to peer") ++
          CliSend(data) ~> state.peers.getById(clientId).netClient
      } else {

        log(s"received Rep from clientId: $clientId, send reply to client") ++
          SrvSend(clientId, data) ~> state.netServer
      }

    // -------------------- SERVER SEND -------------------------//
   // case send: ServerSend => send ~> state.netServer

    case SrvMessage(_, data) => Cmd(data) ~> ref

  }

  // -----------------------HELPERS------------------------------- //

  private def sendPropose: DslF[F, Unit] = {
    implicit val correlationId: CorrelationId = CorrelationId()
    state.peers.all
      .map { case (addr, peer) =>
        log(s"send propose to '$addr'") ++ Propose(state.addr, state.num).toClient ~> peer.netClient
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
        log(s"send heartbeat to $addr") ++ Heartbeat(state.addr, state.leader).toClient ~> peer.netClient
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
    val step0 = handleError(monitorCluster, err => eval(logger.error("cluster monitor has failed", err)))
    def step: DslF[F, Unit] = flow {
      step0 ++ delay(state.delays.monitor) ++ step
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
               val id: String,
               val addr: Addr,
               val netServer: ProcessRef,
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

  sealed trait Phase
  case object Coordinator extends Phase
  case object Leader extends Phase

  sealed trait InternalApi extends Event
  case object Begin extends InternalApi
  case class Timeout(phase: Phase) extends InternalApi

  // internal API
  case class Broadcast(data: Array[Byte]) extends InternalApi
  case class BroadcastResult(majorityCount: Int) extends InternalApi
  case object IsLeader extends InternalApi
  case class IsLeaderRep(leader: Boolean) extends InternalApi

  implicit class CmdOps(e: Api) {
    def toClient: Event = CliSend(e.toByteArray)
  }

  class Peer(
              val id: String,
              val address: Addr,
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
      s"addr=$address, netClientRef=$netClient"
  }

  case class Peers(peers: Vector[Peer]) {
    private val map: Map[Addr, Peer] = peers.map(p => p.address -> p).toMap
    private val idMap: Map[Addr, Peer] = peers.map(p => p.id -> p).toMap
    val netClients: Seq[NetClient] = peers.map(_.netClient)

    def all: Map[Addr, Peer] = map

    @throws[IllegalStateException]
    def getById(id: Addr): Peer = idMap.get(id) match {
      case Some(value) => value
      case None => throw new IllegalStateException(s"peer with id=$id doesn't exist")
    }

    @throws[IllegalStateException]
    def get(addr: Addr): Peer = map.get(addr) match {
      case Some(value) => value
      case None => throw new IllegalStateException(s"peer with addr=$addr doesn't exist")
    }

    def idExists(id: String): Boolean = idMap.contains(id)

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
        s"{peer=${v.address}, status=$status}"
      }
      .mkString("; ")
  }

  object Peers {

    def builder: Builder = new Builder()

    class Builder {
      private var _id: String = _
      private var _address: String = _
      private var _netClient: NetClient = _
      private var _timeoutMs = 10000L
      private var _clock: Clock = Clock()

      def id(id: String): Builder = {
        _id = id
        this
      }

      def address(address: String): Builder = {
        _address = address
        this
      }

      def netClient(netClient: NetClient): Builder = {
        _netClient = netClient
        this
      }

      def timeoutMs(timeoutMs: FiniteDuration): Builder = {
        _timeoutMs = timeoutMs.toMillis
        this
      }

      def clock(clock: Clock): Builder = {
        _clock = clock
        this
      }

      def build: Peer = {
        require(Option(_id).exists(_.nonEmpty), "id cannot be null or empty")
        require(Option(_address).exists(_.nonEmpty), "address cannot be null or empty")
        require(Option(_netClient).isDefined, "netClient cannot be null")
        require(Option(_timeoutMs).exists(_ > 0), "timeout must be > 0")
        require(Option(_clock).isDefined, "clock cannot be null")
        new Peer(id = _id,
          address = _address,
          netClient = _netClient,
          timeoutMs = _timeoutMs,
          clock = _clock)
      }
    }

  }

}
