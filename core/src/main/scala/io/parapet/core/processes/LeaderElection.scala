package io.parapet.core.processes


import cats.effect.Concurrent
import com.typesafe.scalalogging.Logger
import io.parapet.core.Clock
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events.Start
import io.parapet.core.api.Cmd
import io.parapet.core.api.Cmd.leaderElection._
import io.parapet.core.api.Cmd.{netClient, netServer}
import io.parapet.core.processes.LeaderElection._
import io.parapet.core.utils.CorrelationId
import io.parapet.{Event, ProcessRef}
import org.slf4j.MDC
import org.slf4j.event.Level

import scala.concurrent.duration._
import scala.util.Random

class LeaderElection[F[_] : Concurrent](override val ref: ProcessRef,
                                        state: State,
                                        sink: ProcessRef = ProcessRef.NoopRef)
  extends ProcessWithState[F, State](state) {

  import dsl._

  private val logger = Logger[LeaderElection[F]]

  override def handle: Receive = {
    case Start => heartbeatLoopAsync ++ monitorClusterLoopAsync

    // ------------------------BEGIN------------------------------ //
    case Begin =>
      implicit val correlationId: CorrelationId = CorrelationId()
      if (state.peers.hasMajority) {
        if (!state.waitForLeader && !state.hasLeader) {
          log("start new election round") ++
            eval(state.waitForLeader(true)) ++ Cmd.coordinator.Start ~> state.coordinatorRef
        } else {
          log("already waiting for a coordinator")
        }
      } else {
        log("cluster is not complete", Level.WARN)
      }

    case Cmd.coordinator.Elected(coordinator) =>
      implicit val correlationId: CorrelationId = CorrelationId()
      (if (coordinator == state.id && (!state.hasLeader || !state.clusterComplete)) {
        for {
          leaderAddress <- eval(state.roulette(state.peers.alive.map(_.address)))
          _ <- log("became coordinator")
          _ <- Announce(state.addr) ~> state.peers.get(leaderAddress).ref
          _ <- log(s"send announce to $leaderAddress")
        } yield ()
      } else {
        log(s"coordinator=$coordinator")
      }) ++ waitForHeartbeat

    // -----------------------ANNOUNCE---------------------------- //
    case Announce(sender) =>
      implicit val correlationId: CorrelationId = CorrelationId()
      val update = Req(state.id, LeaderUpdate(state.id, state.addr).toByteArray)
      log(s"received Announce(sender=$sender)") ++
        eval {
          require(state.leader.isEmpty, "current leader should be discarded")
          state.leader(state.addr)
        } ++ update ~> sink ++ sendToPeers(update)

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
      log(s"received Heartbeat from $sender, leader=$leader)") ++
        eval(state.peers.get(sender).update()) ++ eval {
        if (heartbeatFromLeader) {
          logUnsafe("received heartbeat from a leader node")
        }
        leader match {
          case Some(leaderAddr) =>
            state.leader match {
              case None =>
                if (state.clusterComplete && heartbeatFromLeader) {
                  state.leader(leaderAddr)
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
                  logUnsafe(s"current leader: $currLeader")
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
      netServer.Send(clientId,
        WhoRep(state.addr, state.leader.contains(state.addr)).toByteArray) ~> state.netServer

    case IsLeader =>
      withSender(sender => IsLeaderRep(state.leader.contains(state.addr)) ~> sender)

    // -----------------------REQ------------------------------- //
    case req: Req =>
      implicit val correlationId: CorrelationId = CorrelationId()
      log(s"forward req to sink") ++
        req ~> sink

    // ----------------------- REP -------------------------------//
    // Rep is sent by sink process in event of Req
    case Rep(clientId, data) =>
      implicit val correlationId: CorrelationId = CorrelationId()
      if (state.peers.idExists(clientId)) {
        log(s"received Rep from clientId: $clientId, send reply to peer") ++
          netClient.Send(data) ~> state.peers.getById(clientId).ref
      } else {

        log(s"received Rep from clientId: $clientId, send reply to client") ++
          netServer.Send(clientId, data) ~> state.netServer
      }

    // -------------------- SERVER SEND -------------------------//
    case send: netServer.Send => send ~> state.netServer

    case netServer.Message(id, data) =>
      implicit val correlationId: CorrelationId = CorrelationId()
      for {
        cmd <- eval(Cmd(data))
        _ <- log(s"$ref received $cmd from $id") ++ cmd ~> ref
      } yield ()

  }

  // -----------------------HELPERS------------------------------- //

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
        log(s"send heartbeat to peer=$addr") ++ Heartbeat(state.addr, state.leader) ~> peer.ref
      }
      .fold(unit)(_ ++ _)
  }

  private def waitForHeartbeat: DslF[F, Unit] = {
    implicit val correlationId: CorrelationId = CorrelationId()
    for {
      _ <- log("wait for heartbeat")
      _ <- fork(delay(state.timeouts.heartbeat) ++ Timeout(Leader) ~> ref)
    } yield ()
  }

  private[core] def monitorClusterLoopAsync: DslF[F, Unit] = fork(monitorClusterLoop)

  private[core] def monitorClusterLoop: DslF[F, Unit] = {
    val step = handleError(monitorCluster, err => eval(logger.error("cluster monitor has failed", err)))

    def loop: DslF[F, Unit] = flow {
      delay(state.delays.monitor) ++ step ++ loop
    }

    loop
  }

  private[core] def monitorCluster: DslF[F, Unit] = {
    var action = unit
    implicit val correlationId: CorrelationId = CorrelationId()
    try {
      val clusterComplete = state.peers.hasMajority
      action = if (clusterComplete && !state.hasLeader) {
        // we need to reset the state b/c the leader may have crashed
        log(s"cluster is complete but the leader is unknown / unreachable") ++ reset ++ Begin ~> ref
      } else if (!clusterComplete) {
        val msg = if (state.hasLeader) {
          "the leader state: ok"
        } else {
          "the leader state: unknown / unreachable"
        }
        log(s"cluster state: incomplete. $msg") ++ reset
      } else {
        log("cluster state: ok")
      }
    } catch {
      case e: Exception =>
        action = log("unexpected error", Level.ERROR, Option(e))
    }

    action
  }

  private def sendToPeers(cmd: Cmd): DslF[F, Unit] = {
    state.peers.refs.foldLeft(unit)((acc, client) => acc ++ cmd ~> client)
  }

  def reset(implicit correlationId: CorrelationId): DslF[F, Unit] =
    eval(resetUnsafe())

  def resetUnsafe()(implicit correlationId: CorrelationId): Unit = {
    logUnsafe("reset the state and start a new election round")
    state.reset()
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

object LeaderElection {

  case class VoteNum(min: Double, max: Double)

  type Addr = String // network address w/o protocol, i.e. (ip|host):port . it's as a unique identifier of a process
  type NetClient = ProcessRef // a process that performs a network operations

  /**
    * List of timeouts.
    *
    * @param coordinator timeout for [[Cmd.coordinator.Elected]] message
    * @param heartbeat   timeout for a heartbeat message H where H.address == H.leader
    */
  case class Timeouts(coordinator: FiniteDuration = 60.seconds,
                      heartbeat: FiniteDuration = 60.seconds)

  case class Delays(election: FiniteDuration = 10.seconds,
                    heartbeat: FiniteDuration = 5.seconds,
                    monitor: FiniteDuration = 10.seconds)

  class State(val id: String,
              val addr: Addr, // doesn't include protocol
              val netServer: ProcessRef,
              val peers: Peers,
              val coordinatorRef: ProcessRef,
              val roulette: Vector[Addr] => Addr = Roulette,
              val timeouts: Timeouts = Timeouts(),
              val delays: Delays = Delays()) {

    private var _leader = Option.empty[Addr]
    private var _coordinator = false
    private var _waitForCoordinator = false

    def waitForLeader: Boolean = _waitForCoordinator

    def waitForLeader(value: Boolean): Unit = _waitForCoordinator = value

    def leader(address: String): Unit = _leader = Option(address)

    def leader: Option[String] = _leader

    def coordinator(value: Boolean): Unit = _coordinator = value

    def coordinator: Boolean = _coordinator

    def hasLeader: Boolean = _leader.exists(l => addr == l || peers.get(l).isAlive)

    def clusterComplete: Boolean = peers.hasMajority

    def reset(): Unit = {
      _coordinator = false
      _leader = Option.empty
      _waitForCoordinator = false
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
      "voted")

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

  val Roulette: Vector[Addr] => Addr = processes => {
    val r = new Random()
    processes(r.nextInt(processes.size))
  }

  sealed trait Phase

  case object Leader extends Phase

  sealed trait InternalApi extends Event

  case object Begin extends InternalApi

  case class Timeout(phase: Phase) extends InternalApi

  // internal API
  case object IsLeader extends InternalApi

  case class IsLeaderRep(leader: Boolean) extends InternalApi

  class Peer(
              val id: String,
              val address: Addr,
              val ref: ProcessRef,
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
    val refs: Seq[ProcessRef] = peers.map(_.ref)
    val majorityCount: Int = (peers.size + 1) / 2 + 1

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

    def ref(address: Addr): NetClient = get(address).ref

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

  object Peer {

    def builder: Builder = new Builder()

    class Builder {
      private var _id: String = _
      private var _address: String = _
      private var _ref: ProcessRef = _
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

      def ref(ref: ProcessRef): Builder = {
        _ref = ref
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
        require(Option(_ref).isDefined, "ref cannot be null")
        require(Option(_timeoutMs).exists(_ > 0), "timeout must be > 0")
        require(Option(_clock).isDefined, "clock cannot be null")
        new Peer(id = _id,
          address = _address,
          ref = _ref,
          timeoutMs = _timeoutMs,
          clock = _clock)
      }
    }

  }

}
