package io.parapet.core.processes

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.Start
import io.parapet.core.processes.RouletteLeaderElection.ResponseCodes.AckCode
import io.parapet.core.processes.RouletteLeaderElection._
import io.parapet.core.{Clock, Encoder, Event, ProcessRef}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Random

// This class implements the modified version of https://arxiv.org/ftp/arxiv/papers/1703/1703.02247.pdf
// lemmas:
// todo associate peer with net client
// a cluster with elected leader receives propose (should reject)
// if quorum is not complete, reset the leader. one quorum is complete - start election
// monitor the number of active nodes. if the number of live nodes < nodes.size / 2 then discard the leader
// assumption: messages delivered in sent order
// questions:
// 1) a process generated num > threshold and then received Propose
// 2) verify else if (num > state.roundNum) in Propose
// ideas:
// 1. remove Heartbeat message and use Ping instead with a flag representing if a sender is a leader
// manual testing cases, cluster 3 nodes:
// 1) kill a random node , then kill the leader, last node should print: 'cluster is not complete. leader is not reachable'
// 2) kill a two non leader nodes, leader should print: 'cluster is not complete. leader is reachable'
// 3) kill the leader: both nodes should print: 'quorum is complete. start election'
// TODO: leader crashed and quickly recovered
class RouletteLeaderElection[F[_]](state: State) extends ProcessWithState[F, State](state) {

  import dsl._

  override val ref: ProcessRef = state.ref

  private val opCounter = new AtomicInteger()

  val logFields = Seq(
    "addr",
    "num",
    "roundNum",
    "leader",
    "votes",
    "coordinator",
    "peerNum",
    "voted"
  )

  override def handle: Receive = {
    case Start => heartbeatLoopAsync ++ monitorClusterLoopAsync

    // ------------------------BEGIN------------------------------ //
    case Begin =>
      if (state.peers.hasMajority) {
        if (!state.voted) { // this process received a Propose message in the current round
          for {
            num <- eval {
              val num = state.random(state.genNumAttempts)
              state.num = num.max
              state.round = state.round + 1
              num
            }
            _ <- if (num.min > state.threshold) {
              eval {
                state.votes = 1 // vote for itself
                state.roundNum = state.num
              } ++ sendPropose ++ waitForCoordinator
            } else {
              debug(s"failed to generate num > threshold. actual: ${num.min}")
            }
          } yield ()

        } else {
          debug("already voted in this round")
        }
      } else {
        debug(s"Warning: event Begin ; quorum is not complete. alive peers = ${state.peers.info}")
      }

    // --------------------PROPOSE--------------------------- //
    case Propose(addr, num) =>
      val peer = state.peers.get(addr)
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

      debug(s"received Propose($addr, $num)") ++ action

    // -----------------------ACK---------------------------- //
    case ack@Ack(peerAddr, num, code) =>
      val action = debug(s"received $ack") ++
        (code match {
          case AckCode.OK =>
            eval {
              state.votes = state.votes + 1
              state.peerNum += (peerAddr -> num)
            } ++
              (
                if (!state.coordinator && hasVotesMajority) {
                  for {
                    leader <- eval(state.roulette(state.peers.alive.map(_.netClient)))
                    _ <- eval(state.coordinator = true)
                    _ <- debug("became coordinator")
                    _ <- Announce(state.addr) ~> leader
                    _ <- debug(s"send announce to $leader")
                    _ <- waitForHeartbeat
                  } yield ()
                } else {
                  unit
                })
          case AckCode.COORDINATOR =>
            debug(s"process '$peerAddr' is already coordinator")
          case AckCode.VOTED =>
            debug(s"process '$peerAddr' has already voted")
          case AckCode.HIGH =>
            debug(s"process '$peerAddr' has the highest number")
          case AckCode.ELECTED =>
            debug("leader already elected")
        })

      action

    // -----------------------ANNOUNCE---------------------------- //
    case a@Announce(_) =>
      debug(s"received $a from coordinator") ++
        eval {
          require(state.leader.isEmpty, "current leader should be discarded")
          state.leader = Option(state.addr)
        }

    // -----------------------TIMEOUT(COORDINATOR)----------------------- //
    case Timeout(Coordinator, ts) =>
      if (!state.coordinator) {
        debug(s"did not receive majority votes to become a coordinator, ts=$ts") ++ reset
      } else {
        unit
      }

    // -----------------------TIMEOUT(LEADER)--------------------------------- //
    case Timeout(Leader, ts) =>
      if (state.leader.isEmpty) {
        debug(s"leader is not available, ts=$ts") ++ reset
      } else {
        unit
      }

    // --------------------HEARTBEAT------------------------------ //
    case Heartbeat(addr, leader) =>
      eval {
        println(createLogMsg(s"received Heartbeat(addr=$addr, leader=$leader)"))
        state.peers.get(addr).update()
      } ++ eval {
        leader match {
          case Some(leaderAddr) =>
            // debug
            if (addr == leaderAddr) {
              println(createLogMsg(s"received Heartbeat($addr) from leader"))
            }
            state.leader match {
              case None if state.quorumComplete && addr == leaderAddr =>
                state.leader = Option(leaderAddr)
                println(createLogMsg(s"new leader '$leaderAddr' elected"))
              case Some(currLeader) if currLeader != leaderAddr =>
                val msg = createLogMsg(s"WARNING: new leader '$leaderAddr', current = '${state.leader}'. the current leader must be discarded")
                println(msg)
                throw new IllegalStateException(msg)
              case _ => () // todo debug
            }
          case None =>
            state.leader match {
              case Some(leaderAddr) if leaderAddr == addr =>
                println(createLogMsg(s"leader $leaderAddr crashed and recovered"))
                reset0()
              case _ => ()
            }
        }
      }
  }

  // -----------------------HELPERS------------------------------- //

  private def sendPropose: DslF[F, Unit] = {
    state.peers.all.map {
      case (addr, peer) => debug(s"send propose to $addr") ++ Propose(state.addr, state.num) ~> peer.netClient
    }.fold(unit)(_ ++ _)
  }

  private def heartbeatLoopAsync: DslF[F, Unit] = fork(heartbeatLoop)

  private def heartbeatLoop: DslF[F, Unit] = {
    def step: DslF[F, Unit] = flow {
      sendHeartbeat ++ delay(state.delays.heartbeat) ++ step
    }

    step
  }

  private[core] def sendHeartbeat: DslF[F, Unit] = {
    state.peers.all.map {
      case (addr, peer) => debug(s"send heartbeat to $addr") ++ Heartbeat(state.addr, state.leader) ~> peer.netClient
    }.fold(unit)(_ ++ _)
  }


  private def waitForCoordinator: DslF[F, Unit] = {
    for {
      _ <- eval(println(createLogMsg(s"wait for majority or heartbeat from leader")))
      _ <- fork(delay(state.timeouts.coordinator) ++ Timeout(Coordinator, 0) ~> ref)
    } yield ()
  }

  private[core] def hasVotesMajority: Boolean = state.votes >= state.peers.size / 2 + 1

  private def waitForHeartbeat: DslF[F, Unit] = {
    for {
      _ <- debug("wait for heartbeat")
      _ <- fork(delay(state.timeouts.heartbeat) ++ Timeout(Leader, 0) ~> ref)
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
    var action = eval(println("monitorCluster: no action"))

    try {
      val quorumComplete = state.peers.hasMajority

      action = if (quorumComplete && !state.hasLeader) {
        // we need to reset the state b/c the leader may have crashed
        debug(s"quorum is complete. start election") ++ reset ++ Begin ~> ref
      } else if (!quorumComplete) {
        val msg = if (state.hasLeader) {
          "leader is still reachable"
        } else {
          "leader is not reachable"
        }
        debug(s"quorum is not complete. $msg") ++ reset
      } else {
        debug("cluster is healthy")
      }
    } catch {
      case e: Exception =>
        action = eval(e.printStackTrace())
    }

    eval(println("monitorCluster:\n")) ++ action
  }

  def reset: DslF[F, Unit] = {
    debug("reset state, start a new round") ++ eval(reset0())
  }

  def reset0(): Unit = {
    state.votes = 0
    state.num = 0.0
    state.roundNum = 0.0
    state.coordinator = false
    state.peerNum.clear()
    state.leader = Option.empty[String]
    state.voted = false
  }

  def debug(msg: String): DslF[F, Unit] = {
    eval(println(createLogMsg(msg)))
  }

  def createLogMsg(msg: String): String = {
    val values = classOf[State].getDeclaredFields.map { field =>
      field.setAccessible(true)
      field.getName -> field.get(state)
    }.toMap

    logFields.foldLeft(new StringBuilder("Log-").append(opCounter.getAndIncrement()).append("\n"))((b, name) =>
      b.append("--| ")
        .append(name).append("=").append(Option(values(name)) match {
        case Some(value) => value match {
          case _: Array[_] => value.asInstanceOf[Array[_]].mkString(",")
          case _ => value.toString
        }
        case None => "null"
      }).append("\n"))
      .append("message=").append(msg).append("\n")
      .append("peers=").append(state.peers.info).append("\n")
      .append("\n\n\n").toString()
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
               val roulette: Vector[ProcessRef] => ProcessRef = Roulette,
               val timeouts: Timeouts = Timeouts(),
               val delays: Delays = Delays(),
               val genNumAttempts: Int = GenNumAttempts,
               val threshold: Double = GenNumThreshold) {
    var num: Double = 0.0
    var roundNum: Double = 0.0
    var votes = 0
    var round = 0
    val peerNum: mutable.Map[String, Double] = mutable.Map.empty // peer addr to num
    var leader = Option.empty[Addr]
    // this flag indicates that this process was chosen as coordinator
    var coordinator = false
    // this flag indicates that this process has already voted in the current round
    var voted = false

    def hasLeader: Boolean = leader.exists(l => addr == l || peers.get(l).isAlive)

    def quorumComplete: Boolean = peers.hasMajority
  }

  val GenNumAttempts = 1
  val GenNumThreshold = 0.85

  val RandomNumGen: RandomNum = i => {
    val r = new Random()
    var min = 1.0
    var max = 0.0
    (0 until i).foreach(_ => {
      val n = r.nextDouble()
      min = Math.min(min, n)
      max = Math.max(max, n)
    })

    VoteNum(min, max)
  }

  val Roulette: Vector[ProcessRef] => ProcessRef = processes => {
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
  case class Timeout(phase: Phase, ts: Long = 0) extends API

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

  // protocol [int32 - tag, body]

  // tags

  val PROPOSE_TAG = 1
  val ACK_TAG = 2
  val ANNOUNCE_TAG = 3
  val HEARTBEAT_TAG = 4

  val encoder = new Encoder {
    override def write(e: Event): Array[Byte] = {
      e match {
        case Propose(addr, num) =>
          val refData = addr.getBytes()
          ByteBuffer.allocate(4 + 4 + refData.length + 8)
            .putInt(PROPOSE_TAG)
            .putInt(refData.length)
            .put(refData)
            .putDouble(num).array()
        case Ack(addr, num, code) =>
          val refData = addr.getBytes()
          ByteBuffer.allocate(4 + 4 + refData.length + 8 + 4)
            .putInt(ACK_TAG)
            .putInt(refData.length)
            .put(refData)
            .putDouble(num)
            .putInt(code.value)
            .array()
        case Announce(addr) =>
          val refData = addr.getBytes()
          ByteBuffer.allocate(4 + 4 + refData.length)
            .putInt(ANNOUNCE_TAG)
            .putInt(refData.length)
            .put(refData)
            .array()
        case Heartbeat(addr, leader) =>
          val addrData = addr.getBytes()
          val leaderAddrData = leader.getOrElse("").getBytes()
          ByteBuffer.allocate(4 + (4 + addrData.length) + (4 + leaderAddrData.length))
            .putInt(HEARTBEAT_TAG)
            .putInt(addrData.length)
            .put(addrData)
            .putInt(leaderAddrData.length)
            .put(leaderAddrData)
            .array()
      }
    }

    override def read(data: Array[Byte]): Event = {
      val buf = ByteBuffer.wrap(data)
      val tag = buf.getInt()
      tag match {
        case PROPOSE_TAG =>
          val refData = new Array[Byte](buf.getInt())
          buf.get(refData)
          val num = buf.getDouble
          Propose(new String(refData), num)

        case ACK_TAG =>
          val refData = new Array[Byte](buf.getInt())
          buf.get(refData)
          val num = buf.getDouble
          val code = buf.getInt
          Ack(new String(refData), num, AckCode(code))
        case ANNOUNCE_TAG =>
          val refData = new Array[Byte](buf.getInt())
          buf.get(refData)
          Announce(new String(refData))
        case HEARTBEAT_TAG =>
          val addrData = new Array[Byte](buf.getInt())
          buf.get(addrData)
          val leaderAddrData = new Array[Byte](buf.getInt())
          buf.get(leaderAddrData)
          Heartbeat(new String(addrData), Option(new String(leaderAddrData)).filter(_.nonEmpty))

      }
    }
  }

  def boolToShort(b: Boolean): Short = {
    if (b) 1
    else 0
  }

  def shortToBool(s: Short): Boolean = s == 1


  class Peer(
              val addr: Addr,
              val netClient: NetClient,
              val timeoutMs: Long,
              val clock: Clock
            ) {
    private var lastPingAt: Long = 0

    def update(): Unit = {
      update(clock.currentTimeMillis)
    }

    private[core] def update(pingAt: Long): Unit = {
      lastPingAt = pingAt
    }

    def isAlive: Boolean = isAlive(clock.currentTimeMillis)

    private[core] def isAlive(cur: Long): Boolean = {
      lastPingAt > cur - timeoutMs
    }

    override def toString: String = {
      s"addr=$addr, netClientRef=$netClient"
    }
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

    def info: String = map.map {
      case (_, v) =>
        val status = if (v.isAlive) "alive" else "unavailable"
        s"{peer=${v.addr}, status=$status}"
    }.mkString("; ")
  }

  object Peers {
    def apply(peers: Map[Addr, NetClient], timeoutMs: Long = 10000, clock: Clock = Clock()): Peers = {
      new Peers(peers.map {
        case (peerAddr, netClient) => new Peer(peerAddr, netClient, timeoutMs, clock)
      }.toVector)
    }
  }

}
