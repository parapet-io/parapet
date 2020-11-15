package io.parapet.core.processes

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.Start
import io.parapet.core.processes.RouletteLeaderElection.ResponseCodes.AckCode
import io.parapet.core.processes.RouletteLeaderElection._
import io.parapet.core.{Encoder, Event, ProcessRef}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Random

// todo associate peer with net client
// a cluster with elected leader receives propose (should reject)
// if quorum is not complete, reset the leader. one quorum is complete - start election
// monitor the number of active nodes. if the number of live nodes < nodes.size / 2 then discard the leader
class RouletteLeaderElection[F[_]](state: State) extends ProcessWithState[F, State](state) {

  import dsl._

  override val ref: ProcessRef = state.ref

  private val opCounter = new AtomicInteger()

  val logFields = Seq(
    "num",
    "roundNum",
    "leader",
    "votes",
    "coordinator",
    "peerNum",
    "voted",
    "lastHeartbeat"
  )

  override def handle: Receive = {
    case Start => fork(sendPing) ++ Begin ~> ref

    case Begin =>
      if (!state.voted) {
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
            eval(println(s"failed to generate num > threshold: ${num.min}")) ++
              fork(delay(new Random().nextInt(3000).millis) ++ Begin ~> ref) // retry
          }
        } yield ()

      } else {
        unit
      }

    case Propose(peerAddr, num) =>
      val peer = state.peers(peerAddr)
      val action =
        if (state.coordinator) {
          Ack(state.addr, state.num, AckCode.COORDINATOR) ~> peer
        } else if (state.voted) {
          Ack(state.addr, state.num, AckCode.VOTED) ~> peer
        } else if (num > state.roundNum) {
          eval {
            state.voted = true
            state.roundNum = num
          } ++ Ack(state.addr, state.num, AckCode.OK) ~> peer ++ waitForHeartbeat
        } else {
          Ack(state.addr, state.num, AckCode.HIGH) ~> peer
        }

      debug(s"received Propose($peerAddr, $num)") ++ action

    case ack@Ack(peerAddr, num, code) =>
      val action =
        debug(s"received $ack") ++
          (if (code == AckCode.OK) {
            eval {
              state.votes = state.votes + 1
              state.peerNum += (peerAddr -> num)
            } ++
              (
                if (!state.coordinator && hasMajority) {
                  for {
                    leader <- eval(state.roulette(state.peers.filterKeys(state.peerNum.contains).values.toVector))
                    _ <- eval(state.coordinator = true)
                    _ <- debug("became coordinator")
                    _ <- Announce(state.addr) ~> leader
                    _ <- debug(s"send announce to $leader")
                    _ <- waitForHeartbeat
                  } yield ()
                } else {
                  unit
                })

          } else {
            eval {
              state.roundNum = num
              state.votes = 0
            } ++ debug(s"process '$peerAddr' has highest number. wait for heartbeat")
          })

      action

    case a@Announce(_) =>
      debug(s"received $a") ++
        eval {
          state.lastHeartbeat = System.nanoTime()
          state.leader = ref
        } ++ sendHeartbeat

    // both timeout are identical
    case Timeout(Coordinator, ts) =>
      if (state.lastHeartbeat == ts) {
        debug(s"did not receive majority, leader is not available, ts=$ts") ++ restart
      } else {
        unit
      }

    case Timeout(Leader, ts) =>
      if (state.lastHeartbeat == ts) {
        // todo: debug message
        // if state.leader == null    ->  leader hasn't elected
        // if state.leader != null    ->  leader has gone
        debug(s"leader is not available, ts=$ts") ++ restart
      } else {
        unit
      }

    case Heartbeat(addr) =>
      eval {
        val leader = state.peers(addr)
        val oldTs = state.lastHeartbeat
        state.lastHeartbeat = System.nanoTime()
        println(createLogMsg(s"received Heartbeat($leader), old_ts = $oldTs, new_ts = ${state.lastHeartbeat}"))
        if (state.leader != leader) {
          println(createLogMsg(s"new leader '$leader' elected, old = '${state.leader}'"))
          state.leader = leader
        }
      } ++ waitForHeartbeat

    case Ping(peer) => debug(s"received ping from $peer")
  }

  def sendHeartbeat: Program = flow {
    state.peers.map {
      case (addr, peerRef) => debug(s"send heartbeat to $addr") ++ Heartbeat(state.addr) ~> peerRef
    }.fold(unit)(_ ++ _) ++ delay(state.timeouts.heartbeat.div(2)) ++ sendHeartbeat
  }

  private[core] def hasMajority: Boolean = state.votes >= state.peers.size / 2 + 1

  private def sendPropose: DslF[F, Unit] = {
    state.peers.map {
      case (addr, peerRef) => debug(s"send propose to $addr") ++ Propose(state.addr, state.num) ~> peerRef
    }.fold(unit)(_ ++ _)
  }

  private def waitForCoordinator: DslF[F, Unit] = {
    for {
      _ <- eval(println(createLogMsg(s"wait for majority or heartbeat from leader")))
      ts <- eval(state.lastHeartbeat) // todo pure
      _ <- fork(delay(state.timeouts.coordinator) ++ Timeout(Coordinator, ts) ~> ref)
    } yield ()
  }

  private def sendPing: DslF[F, Unit] = flow {
    state.peers.map {
      case (addr, peerRef) => debug(s"send ping to $addr") ++ Ping(state.addr) ~> peerRef
    }.fold(unit)(_ ++ _) ++ delay(state.timeouts.ping) ++ sendPing
  }

  private def waitForHeartbeat: DslF[F, Unit] = {
    for {
      _ <- debug("wait for heartbeat")
      ts <- eval(state.lastHeartbeat) // todo pure
      _ <- fork(delay(state.timeouts.heartbeat) ++ Timeout(Leader, ts) ~> ref)
    } yield ()
  }

  private def restart: DslF[F, Unit] = reset ++ Begin ~> ref

  def reset: DslF[F, Unit] =
    debug("reset state, start a new round") ++
      eval {
        state.votes = 0
        state.num = 0.0
        state.roundNum = 0.0
        state.coordinator = false
        state.peerNum.clear()
        state.leader = null
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

    logFields.foldLeft(new StringBuilder("Log-").append(opCounter.getAndIncrement()))((b, name) =>
      b.append("--| ")
        .append(name).append("=").append(Option(values(name)) match {
        case Some(value) => value match {
          case _: Array[_] => value.asInstanceOf[Array[_]].mkString(",")
          case _ => value.toString
        }
        case None => "null"
      }).append("\n"))
      .append("message=").append(msg).append("\n\n\n").toString()
  }

}

object RouletteLeaderElection {

  case class VoteNum(min: Double, max: Double)

  type RandomNum = Int => VoteNum

  case class Timeouts(coordinator: FiniteDuration, heartbeat: FiniteDuration, ping: FiniteDuration)

  class State(
               val ref: ProcessRef,
               val addr: String,
               val peers: Map[String, ProcessRef], // net addr -> net-client
               val random: RandomNum = RandomNumGen,
               val roulette: Vector[ProcessRef] => ProcessRef = Roulette,
               val timeouts: Timeouts = Timeouts(coordinator = 30.seconds, heartbeat = 60.seconds, ping = 10.seconds),
               val genNumAttempts: Int = GenNumAttempts,
               val threshold: Double = GenNumThreshold) {
    var num: Double = 0.0
    var roundNum: Double = 0.0
    var votes = 0
    var round = 0
    val peerNum: mutable.Map[String, Double] = mutable.Map.empty // peer addr to num
    var leader: ProcessRef = _
    // this flag indicates that this process was chosen as coordinator
    var coordinator = false
    // this flag indicates that this process has already voted in the current round
    var voted = false

    var lastHeartbeat = 0L // a timestamp when last heartbeat was received
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
  case class Propose(addr: String, num: Double) extends API
  case class Ack(addr: String, num: Double, code: AckCode) extends API
  case class Announce(addr: String) extends API
  case class Heartbeat(addr: String) extends API
  case class Timeout(phase: Phase, ts: Long = 0) extends API
  case class Ping(addr: String) extends API

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
  val PING_TAG = 5

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
        case Heartbeat(addr) =>
          val refData = addr.getBytes()
          ByteBuffer.allocate(4 + 4 + refData.length)
            .putInt(HEARTBEAT_TAG)
            .putInt(refData.length)
            .put(refData)
            .array()
        case Ping(addr) =>
          val refData = addr.getBytes()
          ByteBuffer.allocate(4 + 4 + refData.length)
            .putInt(PING_TAG)
            .putInt(refData.length)
            .put(refData)
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
          val refData = new Array[Byte](buf.getInt())
          buf.get(refData)
          Heartbeat(new String(refData))
        case PING_TAG =>
          val refData = new Array[Byte](buf.getInt())
          buf.get(refData)
          Ping(new String(refData))
      }
    }
  }

  def boolToShort(b: Boolean): Short = {
    if (b) 1
    else 0
  }

  def shortToBool(s: Short): Boolean = s == 1


}
