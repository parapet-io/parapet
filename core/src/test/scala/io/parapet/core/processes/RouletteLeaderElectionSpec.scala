package io.parapet.core.processes

import cats.Id
import io.parapet.core.Clock
import io.parapet.core.TestUtils._
import io.parapet.core.api.Cmd
import io.parapet.core.api.Cmd.leaderElection._
import io.parapet.core.api.Cmd.netClient
import io.parapet.core.processes.RouletteLeaderElection._
import io.parapet.core.processes.RouletteLeaderElectionSpec._
import io.parapet.{Event, ProcessRef}
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.duration._

class RouletteLeaderElectionSpec extends AnyFunSuite {

  test("a node satisfying the launch condition", Lemma1) {

    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p1:6666"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val server = ProcessRef("server")

    val state = new State(p1, "p1",
      p1Addr, server, createPeers(Map(p2Addr -> p2)), _ => VoteNum(0.86, 0.86), threshold = 0.85)
    val execution = new Execution()
    val le = new RouletteLeaderElection[Id](state)
    updatePeers(state)

    // when
    le(Begin).foldMap(IdInterpreter(execution, eventMapper))

    // then
    execution.print()

    execution.trace shouldBe Seq(
      Message(Propose(p1Addr, 0.86), p2),
      Message(Timeout(io.parapet.core.processes.RouletteLeaderElection.Coordinator), p1)
    )
  }

  test("a node receives propose with higher number", Lemma2) {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p1:6666"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val server = ProcessRef("server")

    val state = new State(p1, "p1", p1Addr, server, createPeers(Map(p2Addr -> p2)))
    state.num = 0.6
    updatePeers(state)
    val execution = new Execution()
    val le = new RouletteLeaderElection[Id](state)

    // when
    le(Propose(p2Addr, 0.86)).foldMap(IdInterpreter(execution, eventMapper))

    // then
    execution.print()

    execution.trace.exists {
      case Message(Ack(p1Addr, 0.6, AckCode.Ok), ProcessRef("p2")) => true
      case _ => false
    } shouldBe true
    state.roundNum shouldBe 0.86
    state.num shouldBe 0.6
    state.voted shouldBe true
  }


  test("a node with higher number receives propose", Lemma3) {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val server = ProcessRef("server")

    val state = new State(p1, "p1", p1Addr, server, createPeers(Map(p2Addr -> p2)))
    state.num = 0.86
    state.roundNum = 0.86
    updatePeers(state)
    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    // when
    le(Propose(p2Addr, 0.6)).foldMap(IdInterpreter(execution, eventMapper))

    // then
    execution.print()

    execution.trace.exists {
      case Message(Ack(p1Addr, 0.86, AckCode.High), ProcessRef("p2")) => true
      case _ => false
    } shouldBe true
  }

  test("a node has the majority of positive responses", Lemma4) {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p3Addr = "p3:7777"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val p3 = ProcessRef("p3")
    val server = ProcessRef("server")

    val execution = new Execution()
    val state = new State(p1, "p1", p1Addr, server, createPeers(Map(p2Addr -> p2, p3Addr -> p3)),
      _ => VoteNum(0.86, 0.86), threshold = 0.85, roulette = _ => p3Addr)
    updatePeers(state)
    val le = new RouletteLeaderElection[Id](state)

    // when
    le(Begin).foldMap(IdInterpreter(execution, eventMapper))
    le(Ack(p2Addr, 0.5, AckCode.Ok)).foldMap(IdInterpreter(execution, eventMapper))
    le(Ack(p3Addr, 0.3, AckCode.Ok)).foldMap(IdInterpreter(execution, eventMapper))

    // then
    execution.print()

    state.coordinator shouldBe true
    state.votes shouldBe 3
    state.peerNum shouldBe Map(p2Addr -> 0.5, p3Addr -> 0.3)

    execution.trace shouldBe Seq(
      Message(Propose(p1Addr, 0.86), p2),
      Message(Propose(p1Addr, 0.86), p3),
      Message(Timeout(Coordinator), p1),
      Message(Announce(p1Addr), p3),
      Message(Timeout(Leader), p1)
    )
  }

  test("a coordinator node receives proposal", Lemma5) {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val server = ProcessRef("server")
    val state = new State(p1, "p1", p1Addr, server, createPeers(Map(p2Addr -> p2)))
    state.num = 0.86
    state.roundNum = 0.86
    state.coordinator = true

    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    // when
    le(Propose(p2Addr, 0.87)).foldMap(IdInterpreter(execution, eventMapper))

    // then
    execution.print()

    execution.trace shouldBe Seq(
      Message(Ack(p1Addr, 0.86, AckCode.Coordinator), p2)
    )
  }

  test("a node that already voted receives proposal", Lemma6) {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p3Addr = "p2:7777"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val p3 = ProcessRef("p3")
    val server = ProcessRef("server")

    val state = new State(p1, "p1", p1Addr, server, createPeers(Map(p2Addr -> p2, p3Addr -> p3)))
    state.num = 0.1

    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    // when
    le(Propose(p2Addr, 0.87)).foldMap(IdInterpreter(execution, eventMapper))
    le(Propose(p3Addr, 0.88)).foldMap(IdInterpreter(execution, eventMapper))

    // then
    execution.print()

    state.roundNum shouldBe 0.87
    state.voted shouldBe true

    execution.trace shouldBe Seq(
      Message(Ack(p1Addr, 0.1, AckCode.Ok), p2),
      Message(Timeout(Leader), p1),
      Message(Ack(p1Addr, 0.1, AckCode.Voted), p3),
    )
  }

  test("a node that voted waiting for a leader receives timeout") {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val server = ProcessRef("server")

    val state = new State(p1, "p1", p1Addr, server, createPeers(Map(p2Addr -> p2)))
    state.num = 0.6

    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    // when
    le(Propose(p2Addr, 0.87)).foldMap(IdInterpreter(execution, eventMapper))
    le(Timeout(Leader)).foldMap(IdInterpreter(execution, eventMapper))

    // then
    execution.print()

    execution.trace shouldBe Seq(
      Message(Ack(p1Addr, 0.6, AckCode.Ok), p2),
      Message(Timeout(io.parapet.core.processes.RouletteLeaderElection.Leader), p1)
    )

    state.voted shouldBe false
    state.num shouldBe 0
    state.roundNum shouldBe 0
  }

  test("a candidate node waiting for majority acks receives timeout", Lemma7) {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val server = ProcessRef("server")

    val state = new State(p1, "p1", p1Addr, server, createPeers(Map(p2Addr -> p2)),
      random = _ => VoteNum(0.86, 0.86), threshold = 0.85)
    updatePeers(state)

    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    // when
    le(Begin).foldMap(IdInterpreter(execution, eventMapper))
    le(Timeout(Coordinator)).foldMap(IdInterpreter(execution, eventMapper))

    // then
    execution.print()

    execution.trace shouldBe Seq(
      Message(Propose(p1Addr, 0.86), p2),
      Message(Timeout(Coordinator), p1),
    )

    state.coordinator shouldBe false
    state.num shouldBe 0
    state.roundNum shouldBe 0
  }

  test("a node that received announce", Lemma8) {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"

    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val server = ProcessRef("server")

    val state = new State(p1, "p1", p1Addr, server, createPeers(Map(p2Addr -> p2)))
    updatePeers(state)

    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    // when
    le(Announce(p2Addr)).foldMap(IdInterpreter(execution, eventMapper))
    le.sendHeartbeat.foldMap(IdInterpreter(execution, eventMapper))

    // then
    execution.print()
    execution.trace shouldBe Seq(
      Message(LeaderUpdate("p1:5555"), ProcessRef("parapet-blackhole")),
      Message(Heartbeat(p1Addr, Option(p1Addr)), p2)
    )
  }

  test("a leader crashed and cluster is complete", Lemma9) {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p3Addr = "p3:7777"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val p3 = ProcessRef("p3")
    val server = ProcessRef("server")

    val peerHeartbeatTimeout = 1.second
    val clock = new Clock.Mock(1.second)
    val state = new State(p1, "p1", p1Addr, server,
      createPeers(Map(p2Addr -> p2, p3Addr -> p3), peerHeartbeatTimeout, clock))

    // todo separate test case
    state.peers.alive.map(p => p.address -> p.netClient).toMap shouldBe Map(p2Addr -> p2, p3Addr -> p3)

    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    // when
    le(Heartbeat(p2Addr, Option(p2Addr))).foldMap(IdInterpreter(execution, eventMapper))
    state.leader shouldBe Some(p2Addr) // todo separate test case
    state.hasLeader shouldBe true // todo separate test case
    clock.tick(2.second)
    state.leader shouldBe Some(p2Addr)
    state.hasLeader shouldBe false
    state.peers.get(p3Addr).update(clock.currentTimeMillis)
    le.monitorCluster.foldMap(IdInterpreter(execution, eventMapper))

    // then
    execution.print()
    state.peers.alive.map(p => p.address -> p.netClient).toMap shouldBe Map(p3Addr -> p3)
    state.leader shouldBe Option.empty
    execution.trace shouldBe Seq(
      Message(Begin, p1)
    )
  }

  test("a leader crashed and cluster is not complete", Lemma9) {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p3Addr = "p3:7777"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val p3 = ProcessRef("p3")
    val server = ProcessRef("server")

    val peerHeartbeatTimeout = 1.second
    val clock = new Clock.Mock(1.second)
    val state = new State(p1, "p1", p1Addr, server,
      createPeers(Map(p2Addr -> p2, p3Addr -> p3), peerHeartbeatTimeout, clock))

    // todo separate test case
    state.peers.alive.map(p => p.address -> p.netClient).toMap shouldBe Map(p2Addr -> p2, p3Addr -> p3)

    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    // when
    le(Heartbeat(p2Addr, Option(p2Addr))).foldMap(IdInterpreter(execution, eventMapper))
    state.leader shouldBe Some(p2Addr) // todo separate test case
    state.hasLeader shouldBe true // todo separate test case
    clock.tick(2.second)
    state.leader shouldBe Some(p2Addr)
    state.hasLeader shouldBe false
    le.monitorCluster.foldMap(IdInterpreter(execution, eventMapper))

    // then
    execution.print()
    state.peers.alive shouldBe Vector.empty
    state.leader shouldBe Option.empty
    execution.trace shouldBe Seq.empty
  }

  test("a node sends propose to healthy cluster") {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p3Addr = "p2:7777"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val p3 = ProcessRef("p3")
    val server = ProcessRef("server")
    val peerHeartbeatTimeout = 1.second
    val clock = new Clock.Mock(1.second)
    val state = new State(p1, "p1", p1Addr, server,
      createPeers(Map(p2Addr -> p2, p3Addr -> p3), peerHeartbeatTimeout, clock))

    state.leader = Option(p2Addr)
    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()
    // when

    le(Propose(p3Addr, 0.9)).foldMap(IdInterpreter(execution, eventMapper))
    // then
    execution.print()
    execution.trace shouldBe Seq(
      Message(Ack(p1Addr, 0.0, AckCode.Elected), p3)
    )
  }

  test("a node joins complete cluster with active leader and receives heartbeat from leader", Lemma11) {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p3Addr = "p2:7777"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val p3 = ProcessRef("p3")
    val server = ProcessRef("server")

    val peerHeartbeatTimeout = 1.second
    val clock = new Clock.Mock(1.second)
    val state = new State(p1, "p1", p1Addr, server,
      createPeers(Map(p2Addr -> p2, p3Addr -> p3), peerHeartbeatTimeout, clock))

    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    // when
    le(Heartbeat(p3Addr, Some(p3Addr))).foldMap(IdInterpreter(execution, eventMapper))

    // then
    execution.print()
    state.peers.alive.map(p => p.address -> p.netClient).toMap shouldBe Map(p2Addr -> p2, p3Addr -> p3)
    state.leader shouldBe Some(p3Addr)
    execution.trace shouldBe Seq.empty
  }

  test("a node joins incomplete cluster with active leader and receives heartbeat from leader", Lemma11) {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p3Addr = "p3:7777"
    val p4Addr = "p4:8888"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val p3 = ProcessRef("p3")
    val p4 = ProcessRef("p4")
    val server = ProcessRef("server")
    val peerHeartbeatTimeout = 1.second
    val clock = new Clock.Mock(1.second)
    val state = new State(p1, "p1", p1Addr, server,
      createPeers(Map(p2Addr -> p2, p3Addr -> p3, p4Addr -> p4), peerHeartbeatTimeout, clock))

    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    // when
    clock.tick(1.second)
    le(Heartbeat(p3Addr, Some(p3Addr))).foldMap(IdInterpreter(execution, eventMapper))

    // then
    execution.print()
    state.peers.alive.map(p => p.address -> p.netClient).toMap shouldBe Map(p3Addr -> p3)
    state.leader shouldBe Option.empty
    execution.trace shouldBe Seq.empty
  }

  test("a node joins complete cluster with active leader and receives heartbeat from non leader", Lemma11) {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p3Addr = "p3:7777"
    val p4Addr = "p4:8888"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val p3 = ProcessRef("p3")
    val p4 = ProcessRef("p4")
    val server = ProcessRef("server")
    val peerHeartbeatTimeout = 1.second
    val clock = new Clock.Mock(1.second)
    val state = new State(p1, "p1", p1Addr, server,
      createPeers(Map(p2Addr -> p2, p3Addr -> p3, p4Addr -> p4), peerHeartbeatTimeout, clock))

    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    // when
    clock.tick(1.second)
    state.peers.all.values.foreach(_.update(clock.currentTimeMillis))
    le(Heartbeat(p3Addr, Some(p2Addr))).foldMap(IdInterpreter(execution, eventMapper))

    // then
    execution.print()
    state.peers.alive.map(p => p.address -> p.netClient).toMap shouldBe Map(p2Addr -> p2, p3Addr -> p3, p4Addr -> p4)
    state.leader shouldBe Option.empty
    execution.trace shouldBe Seq.empty
  }

  test("a node received a heartbeat with a different leader") {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p3Addr = "p3:6666"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val p3 = ProcessRef("p3")
    val server = ProcessRef("server")
    val state = new State(p1, "p1", p1Addr, server, createPeers(Map(p2Addr -> p2, p3Addr -> p3)))
    state.leader = Some(p2Addr)
    val le = new RouletteLeaderElection[Id](state)
    val execution = new Execution()

    // when
    assertThrows[IllegalStateException] {
      le(Heartbeat(p3Addr, Some(p3Addr))).foldMap(IdInterpreter(execution, eventMapper))
    }

  }

  test("leader crashed and joined the cluster", Lemma12) {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val server = ProcessRef("server")
    val state = new State(p1, "p1", p1Addr, server, createPeers(Map(p2Addr -> p2)))
    state.voted = true
    state.leader = Some(p2Addr)

    val execution = new Execution()
    val le = new RouletteLeaderElection[Id](state)

    // when
    le(Heartbeat(p2Addr, Option.empty)).foldMap(IdInterpreter(execution, eventMapper))

    // then

    state.leader shouldBe Option.empty
  }

}

object RouletteLeaderElectionSpec {

  import io.parapet.core.doc.RouletteLeaderElectionDoc.Lemmas

  // @formatter:off
  object Lemma1  extends  Tag(Lemmas.Lemma1.description)
  object Lemma2  extends  Tag(Lemmas.Lemma2.description)
  object Lemma3  extends  Tag(Lemmas.Lemma3.description)
  object Lemma4  extends  Tag(Lemmas.Lemma4.description)
  object Lemma5  extends  Tag(Lemmas.Lemma5.description)
  object Lemma6  extends  Tag(Lemmas.Lemma6.description)
  object Lemma7  extends  Tag(Lemmas.Lemma7.description)
  object Lemma8  extends  Tag(Lemmas.Lemma8.description)
  object Lemma9  extends  Tag(Lemmas.Lemma9.description)
  object Lemma10 extends  Tag(Lemmas.Lemma10.description)
  object Lemma11 extends  Tag(Lemmas.Lemma11.description)
  object Lemma12 extends  Tag(Lemmas.Lemma12.description)
  // @formatter:on

  val eventMapper: Event => Event = {
    case netClient.Send(bytes, None) => Cmd(bytes)
    case e => e
  }

  def updatePeers(s: State, ts: Long = System.currentTimeMillis()): Unit = {
    s.peers.peers.foreach(_.update(ts))
  }

  def createPeers(peers: Map[String, ProcessRef], timeout: FiniteDuration = 10000.millis, clock: Clock = Clock()): Peers =
    Peers(peers.map(p =>
      Peers.builder.id(p._2.value)
        .address(p._1)
        .netClient(p._2)
        .timeoutMs(timeout)
        .clock(clock)
        .build).toVector)

}