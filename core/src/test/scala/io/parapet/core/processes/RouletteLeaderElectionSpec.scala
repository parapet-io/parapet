package io.parapet.core.processes

import cats.Id
import io.parapet.core.Event.Start
import io.parapet.core.ProcessRef
import io.parapet.core.TestUtils._
import io.parapet.core.processes.RouletteLeaderElection.ResponseCodes.AckCode
import io.parapet.core.processes.RouletteLeaderElection._
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.OptionValues._


/**
  * todo:
  * 1. more than once process generates a num > threshold
  * 2. Timeout messages must be created with current ts (lastHeartbeat) value
  */
class RouletteLeaderElectionSpec extends FunSuite {

  test("a node satisfying the launch condition") {
    // given
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")

    val state = new State(p1, Vector(p2), _ => VoteNum(0.86, 0.86), threshold = 0.85)
    val execution = new Execution()
    val le = new RouletteLeaderElection[Id](state)

    // when
    le(Start).foldMap(IdInterpreter(execution))

    // then
    execution.print()

    execution.trace shouldBe Seq(
      Message(Propose(p1, 0.86), p2),
      Message(Timeout(io.parapet.core.processes.RouletteLeaderElection.Coordinator), p1)
    )
  }

  test("a node receives propose with higher number") {
    // given
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")

    val state = new State(p1, Vector(p2))
    state.num = 0.6
    val execution = new Execution()
    val le = new RouletteLeaderElection[Id](state)

    // when
    le(Propose(p2, 0.86)).foldMap(IdInterpreter(execution))

    // then
    execution.print()

    execution.trace.exists {
      case Message(Ack(ProcessRef("p1"), 0.6, AckCode.OK), ProcessRef("p2")) => true
      case _ => false
    } shouldBe true
    state.roundNum shouldBe 0.86
    state.voted shouldBe true
  }

  test("a node with higher num receives proposal") {
    // given
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")

    val state = new State(p1, Vector(p2))
    state.num = 0.86
    state.roundNum = 0.86
    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    // when
    le(Propose(p2, 0.6)).foldMap(IdInterpreter(execution))

    // then
    execution.print()

    execution.trace.exists {
      case Message(Ack(ProcessRef("p1"), 0.86, AckCode.HIGH), ProcessRef("p2")) => true
      case _ => false
    } shouldBe true
  }

  test("a node has the majority of positive responses") {
    // given
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val p3 = ProcessRef("p3")

    val execution = new Execution()
    val state = new State(p1, Vector(p2, p3), _ => VoteNum(0.86, 0.86), threshold = 0.85, roulette = _ => p3)
    val le = new RouletteLeaderElection[Id](state)

    // when
    le(Start).foldMap(IdInterpreter(execution))
    le(Ack(ProcessRef("p2"), 0.5, AckCode.OK)).foldMap(IdInterpreter(execution))
    le(Ack(ProcessRef("p3"), 0.3, AckCode.OK)).foldMap(IdInterpreter(execution))

    // then
    execution.print()

    state.coordinator shouldBe true

    execution.trace shouldBe Seq(
      Message(Propose(p1, 0.86), p2),
      Message(Propose(p1, 0.86), p3),
      Message(Timeout(Coordinator), p1),
      Message(Announce(p1), p3),
      Message(Timeout(Leader), p1)
    )
  }

  test("a coordinator node receives proposal") {
    // given
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val state = new State(p1, Vector(p2))
    state.num = 0.86
    state.roundNum = 0.86
    state.coordinator = true

    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    // when
    le(Propose(p2, 0.87)).foldMap(IdInterpreter(execution))

    // then
    execution.print()

    execution.trace shouldBe Seq(
      Message(Ack(p1, 0.86, AckCode.COORDINATOR), p2)
    )
  }

  test("a node that already voted receives proposal") {
    // given
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val p3 = ProcessRef("p3")

    val state = new State(p1, Vector(p2, p3))
    state.num = 0.86
    state.roundNum = 0.86

    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    // when
    le(Propose(p2, 0.87)).foldMap(IdInterpreter(execution))
    le(Propose(p3, 0.88)).foldMap(IdInterpreter(execution))

    // then
    execution.print()

    execution.trace shouldBe Seq(
      Message(Ack(p1, 0.86, AckCode.OK), p2),
      Message(Timeout(Leader, 0), p1),
      Message(Ack(p1, 0.86, AckCode.VOTED), p3),
    )
  }

  test("a node that voted waiting for a leader receives timeout") {
    // given
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")

    val state = new State(p1, Vector(p2))
    state.num = 0.6

    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    // when
    le(Propose(p2, 0.87)).foldMap(IdInterpreter(execution))
    le(Timeout(Leader)).foldMap(IdInterpreter(execution))

    // then
    execution.print()

    execution.trace shouldBe Seq(
      Message(Ack(p1, 0.6, AckCode.OK), p2),
      Message(Timeout(io.parapet.core.processes.RouletteLeaderElection.Leader), p1),
      Message(Start, p1)
    )

    state.voted shouldBe false
    state.num shouldBe 0
    state.roundNum shouldBe 0
  }

  test("a node that voted waiting for majority acks receives timeout") {
    // given
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")

    val state = new State(p1, Vector(p2), _ => VoteNum(0.86, 0.86))

    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    // when
    le(Start).foldMap(IdInterpreter(execution))
    le(Timeout(Coordinator)).foldMap(IdInterpreter(execution))

    // then
    execution.print()

    execution.trace shouldBe Seq(
      Message(Propose(p1, 0.86), p2),
      Message(Timeout(Coordinator), p1),
      Message(Start, p1)
    )

    state.coordinator shouldBe false
    state.num shouldBe 0
    state.roundNum shouldBe 0
  }

  test("a node received heartbeat with old timestamp") {
    // given
    val p1 = ProcessRef("p1")
    val state = new State(p1, peers = Vector.empty)
    state.roundNum = 1
    state.lastHeartbeat = 1

    // when
    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    le(Timeout(Leader, 1)).foldMap(IdInterpreter(execution))

    // then
    execution.print()
    execution.trace shouldBe Seq(
      Message(Start, p1)
    )
    state.roundNum shouldBe 0
  }

  test("a node received heartbeat with new timestamp") {
    // given
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val state = new State(p1, peers = Vector.empty)
    state.roundNum = 1
    val lastHeartbeat = 1
    state.lastHeartbeat = lastHeartbeat

    // when
    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    le(Heartbeat(p2)).foldMap(IdInterpreter(execution))

    // then
    execution.print()
    execution.trace.size shouldBe 1
    execution.trace.headOption.value should matchPattern {
      case Message(t@Timeout(Leader, _), _) if t.ts == state.lastHeartbeat => ()
    }

    state.lastHeartbeat shouldNot equal(lastHeartbeat)
  }

}
