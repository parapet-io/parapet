package io.parapet.core

import cats.{Id, Monad, ~>}
import io.parapet.core.Dsl.{Dsl, Eval, FlowOp, Send, UnitFlow, WithDsl}
import io.parapet.core.Event.Start
import io.parapet.core.ProcessSpec.LeaderElection._
import io.parapet.core.ProcessSpec._
import io.parapet.syntax.FlowSyntax
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._


class ProcessSpec extends FlatSpec with WithDsl[Id] with FlowSyntax[Id] {

  /*

  "Process with num gt threshold" should "send propose" in {

    val peers = ListBuffer(ProcessRef("p1"), ProcessRef("p2"))

    val state = new LeaderElectionState(
      peers = peers,
      threshold = 0.85,
      random = () => 0.9,
      wheel = () => 0
    )

    val p = new LeaderElection[Id](state)

    val program = p(Start)

    val interpreter = new IdInterpreter()

    program.foldMap(interpreter)

    state.num shouldBe 0.9
    interpreter.execution.trace shouldBe peers.map(Propose(_, state.num))
  }

  "Process with highest num received positive ack" should "be happy" in {

    val peers = ListBuffer(ProcessRef("p1"), ProcessRef("p2"))


    val state = new LeaderElectionState(
      peers = peers,
      threshold = 0.85,
      random = () => 0.9,
      wheel = () => 0
    )
    state.num = 0.9


    val p = new LeaderElection[Id](state)
    val program = peers.map(peer => p(Ack(peer, 0.1, ok = true))).fold(dsl.unit)(_ ++ _)


    val interpreter = new IdInterpreter()

    program.foldMap(interpreter)

    state.num shouldBe 0.9
    state.votes shouldBe 2
    state.peerNum shouldBe peers.map(_ -> 0.1).toMap
  }
  */
}



object ProcessSpec {

  case class VoteNum(min: Double, max: Double)

  type NumGen = Int => VoteNum // attempts => (min, max)

  case class Timeouts(coordinator: Duration, leader: Duration)

  class LeaderElectionState(val peers: ListBuffer[ProcessRef],
                            val threshold: Double = 0.85,
                            val random: NumGen,
                            val roulette: Seq[ProcessRef] => ProcessRef,
                            val timeouts: Timeouts = Timeouts(30.seconds, 30.seconds)) {
    var num: Double = 0.0
    var roundNum: Double = 0.0
    var votes = 0
    var round = 0
    val peerNum: mutable.Map[ProcessRef, Double] = mutable.Map.empty
    var leader: ProcessRef = null
    var coordinator = false
  }


  // two processes generated nums > 0.85, min = max
  // p1(n, rv) == p2(n, rv)
  class LeaderElection[F[_]](state: LeaderElectionState) extends ProcessWithState[F, LeaderElectionState](state) {


    import dsl._

    override def handle: Receive = {
      case Start =>
        val num = state.random(3)
        state.num = num.max
        eval(state.round = state.round + 1) ++
          (if (num.min > state.threshold) {
            eval {
              state.votes = 1 // vote for itself
              state.roundNum = state.num
              println(s"round[${state.round}, $ref] sends proposal(${state.num})")
            } ++ state.peers.map(p => Propose(ref, state.num) ~> p).fold(unit)(_ ++ _)
          } else {
            Start ~> ref // try again
          })

      case Propose(peer, num) =>
        if (state.coordinator) {
          Ack(ref, state.num, ok = false, coordinator = true) ~> peer
        }
        else if (num > state.roundNum) {
          eval(state.roundNum = num) ++
            Ack(ref, state.num, ok = true) ~> peer
        } else {
          Ack(ref, state.num, ok = false) ~> peer
        }
      case Ack(peer, num, ok, _) =>
        eval(println(s"peer[$peer] ack [$num, $ok]")) ++
          (if (ok) {
            for {
              _ <- eval {
                state.votes = state.votes + 1
                state.peerNum += (peer -> num)
                if (state.votes >= state.peers.length / 2 + 1) {
                  val leader = state.roulette(state.peerNum.keys.toList)
                  eval(state.coordinator = true) ++ Announce(ref) ~> leader
                }
                // todo check the majority
              }
            } yield ()
          } else {
            eval {
              state.num = num
              state.votes = 1
              // reset votes ?
            }
          })

      case Announce(coordinator) =>
        eval(state.leader = ref) ++
          state.peers.map(p => Heartbeat(ref) ~> p).fold(unit)(_ ++ _)
      case Timeout(Coordinator) =>
        if (!state.coordinator) eval(reset()) ++ Start ~> ref
        else unit

      case Timeout(Leader) =>
        if (state.leader == null) eval(reset()) ++ Start ~> ref
        else unit

      case Heartbeat(leader) =>
        eval(state.leader = leader)
    }

    def reset(): Unit = {
      state.votes = 0
      state.num = 0.0
      state.roundNum = 0.0
      state.coordinator = false
      state.peerNum.clear()
      state.leader = null
    }

  }

  // api

  object LeaderElection {

    // @formatter:off
    sealed trait Phase
    object Coordinator extends Phase
    object Leader extends Phase

    sealed trait API extends Event
    case class Propose(ref: ProcessRef, num: Double) extends API
    case class Ack(ref: ProcessRef, num: Double, ok: Boolean, coordinator: Boolean = false) extends API
    case class Announce(ref: ProcessRef) extends API
    case class Heartbeat(ref: ProcessRef) extends API
    case class Timeout(phase: Phase) extends API
    // @formatter:on
  }

  // emulator

  abstract class ProcessWithState[F[_], S](state: S) extends io.parapet.core.Process[F]

  class MyProcess extends Process[Id] {

    import dsl._

    override def handle: Receive = {
      case Start => eval(println("test"))
    }

  }

}
