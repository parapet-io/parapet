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

class RouletteLeaderElection[F[_]](state: State) extends ProcessWithState[F, State](state) {

  import dsl._

  override val ref: ProcessRef = state.ref

  private val opCounter = new AtomicInteger()

  override def handle: Receive = {
    case Start =>
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
            fork(delay(new Random().nextInt(3000).millis) ++ Start ~> ref) // retry
          }
        } yield ()

      } else {
        unit
      }

    case Propose(peer, num) =>
      val action =
        if (state.coordinator) {
          Ack(ref, state.num, AckCode.COORDINATOR) ~> peer
        } else if (state.voted) {
          Ack(ref, state.num, AckCode.VOTED) ~> peer
        } else if (num > state.roundNum) {
          eval {
            state.voted = true
            state.roundNum = num
          } ++ Ack(ref, state.num, AckCode.OK) ~> peer ++ waitForHeartbeat
        } else {
          Ack(ref, state.num, AckCode.HIGH) ~> peer
        }

      eval(println(s"$info :: received Propose($peer, $num)")) ++ action

    case Ack(peer, num, code) =>
      val action =
        if (code == AckCode.OK) {
          eval {
            state.votes = state.votes + 1
            state.peerNum += (peer -> num)
            println(s"$info Ack")
          } ++
            (
              if (!state.coordinator && hasMajority) {
                for {
                  leader <- eval(state.roulette(state.peerNum.keys.toVector))
                  _ <- eval {
                    state.coordinator = true
                    println(s"$info became coordinator")
                  }
                  _ <- Announce(ref) ~> leader
                  _ <- eval(println(s"$info send announce to $leader"))
                  _ <- waitForHeartbeat
                } yield ()
              } else {
                unit
              })

        } else {
          eval {
            state.roundNum = num
            state.votes = 0
            // todo what do we need to reset?
          }
        }

      eval(println(s"$info :: received Ack($num, $code)")) ++ action

    case Announce(_) =>
      eval {
        println(s"$info :: received Announce")
        state.lastHeartbeat = System.nanoTime()
        state.leader = ref
      } ++ sendHeartbeat

    // both timeout are identical
    case Timeout(Coordinator, ts) =>
      if (state.lastHeartbeat == ts) {
        eval(println(s"$info did not receive majority, leader is not available, ts=$ts")) ++ restart
      } else {
        unit
      }

    case Timeout(Leader, ts) =>
      if (state.lastHeartbeat == ts) {
        eval(println(s"$info leader is not available, ts=$ts")) ++ restart
      } else {
        unit
      }

    case Heartbeat(leader) =>
      eval {
        val oldTs = state.lastHeartbeat
        state.lastHeartbeat = System.nanoTime()
        println(s"$info received Heartbeat($leader), old_ts = $oldTs, new_ts = ${state.lastHeartbeat}")
        if (state.leader != leader) {
          println(s"$info new leader '$leader' elected, old = '${state.leader}'")
          state.leader = leader
        }
      } ++ waitForHeartbeat
  }

  def sendHeartbeat: Program = flow {
    state.peers.map(p => Heartbeat(ref) ~> p).fold(unit)(_ ++ _) ++
      delay(state.timeouts.heartbeat.div(2)) ++ sendHeartbeat
  }

  private[core] def hasMajority: Boolean = state.votes >= state.peers.length / 2 + 1

  private def sendPropose: DslF[F, Unit] = {
    eval(println(s"$info :: sent proposal")) ++
      state.peers.map(p => Propose(ref, state.num) ~> p).fold(unit)(_ ++ _)
  }

  private def waitForCoordinator: DslF[F, Unit] = {
    for {
      _ <- eval(println(s"$info wait for majority or heartbeat from leader"))
      ts <- eval(state.lastHeartbeat) // todo pure
      _ <- fork(delay(state.timeouts.coordinator) ++ Timeout(Coordinator, ts) ~> ref)
    } yield ()
  }

  private def waitForHeartbeat: DslF[F, Unit] = {
    for {
      _ <- eval(println(s"$info wait for heartbeat"))
      ts <- eval(state.lastHeartbeat) // todo pure
      _ <- fork(delay(state.timeouts.heartbeat) ++ Timeout(Leader, ts) ~> ref)
    } yield ()
  }

  private def restart: DslF[F, Unit] = reset ++ Start ~> ref

  def reset: DslF[F, Unit] = eval {
    println(s"$info :: reset, start new round")
    state.votes = 0
    state.num = 0.0
    state.roundNum = 0.0
    state.coordinator = false
    state.peerNum.clear()
    state.leader = null
    state.voted = false
  }

  def info: String = {
    val parts = mutable.LinkedHashMap(
      "ref" -> ref,
      "c" -> opCounter.getAndIncrement(),
      "round" -> state.round,
      "num" -> state.num,
      "roundNum" -> state.roundNum,
      "coordinator" -> state.coordinator,
      "peerNum" -> state.peerNum,
      "leader" -> state.leader,
      "votes" -> state.votes
    )
    parts.toString()
  }
}

object RouletteLeaderElection {

  case class VoteNum(min: Double, max: Double)

  type RandomNum = Int => VoteNum

  case class Timeouts(coordinator: FiniteDuration, heartbeat: FiniteDuration)

  class State(
               val ref: ProcessRef,
               val peers: Vector[ProcessRef],
               val random: RandomNum = RandomNumGen,
               val roulette: Vector[ProcessRef] => ProcessRef = Roulette,
               val timeouts: Timeouts = Timeouts(coordinator = 30.seconds, heartbeat = 60.seconds),
               val genNumAttempts: Int = GenNumAttempts,
               val threshold: Double = GenNumThreshold) {
    var num: Double = 0.0
    var roundNum: Double = 0.0
    var votes = 0
    var round = 0
    val peerNum: mutable.Map[ProcessRef, Double] = mutable.Map.empty
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
  case class Propose(ref: ProcessRef, num: Double) extends API
  case class Ack(ref: ProcessRef, num: Double, code: AckCode) extends API
  case class Announce(ref: ProcessRef) extends API
  case class Heartbeat(ref: ProcessRef) extends API
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
        case Propose(ref, num) =>
          val refData = ref.ref.getBytes()
          ByteBuffer.allocate(4 + 4 + refData.length + 8)
            .putInt(PROPOSE_TAG)
            .putInt(refData.length)
            .put(refData)
            .putDouble(num).array()
        case Ack(ref, num, code) =>
          val refData = ref.ref.getBytes()
          ByteBuffer.allocate(4 + 4 + refData.length + 8 + 4)
            .putInt(ACK_TAG)
            .putInt(refData.length)
            .put(refData)
            .putDouble(num)
            .putInt(code.value)
            .array()
        case Announce(ref) =>
          val refData = ref.ref.getBytes()
          ByteBuffer.allocate(4 + 4 + refData.length)
            .putInt(ANNOUNCE_TAG)
            .putInt(refData.length)
            .put(refData)
            .array()
        case Heartbeat(ref) =>
          val refData = ref.ref.getBytes()
          ByteBuffer.allocate(4 + 4 + refData.length)
            .putInt(HEARTBEAT_TAG)
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
          Propose(ProcessRef(new String(refData)), num)

        case ACK_TAG =>
          val refData = new Array[Byte](buf.getInt())
          buf.get(refData)
          val num = buf.getDouble
          val code = buf.getInt
          Ack(ProcessRef(new String(refData)), num, AckCode(code))
        case ANNOUNCE_TAG =>
          val refData = new Array[Byte](buf.getInt())
          buf.get(refData)
          Announce(ProcessRef(new String(refData)))
        case HEARTBEAT_TAG =>
          val refData = new Array[Byte](buf.getInt())
          buf.get(refData)
          Heartbeat(ProcessRef(new String(refData)))
      }
    }
  }

  def boolToShort(b: Boolean): Short = {
    if (b) 1
    else 0
  }

  def shortToBool(s: Short): Boolean = s == 1


}
