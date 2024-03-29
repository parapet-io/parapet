package io.parapet.core.processes

import cats.{Eval => CatsEval}
import io.parapet.core.Clock
import io.parapet.core.TestUtils._
import io.parapet.core.api.Cmd
import io.parapet.core.api.Cmd.leaderElection._
import io.parapet.core.api.Cmd.netClient
import io.parapet.core.processes.LeaderElection._
import io.parapet.core.processes.LeaderElectionSpec._
import io.parapet.{Event, ProcessRef}
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.duration._

class LeaderElectionSpec extends AnyFunSuite {

  test("a node received elected received leader timeout", Lemma1) {
    // give
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val server = ProcessRef("server")
    val coordinator = ProcessRef("coordinator")

    val state = new State("p1", p1Addr, server, createPeers(Map(p2Addr -> p2)), coordinator)

    val le = new LeaderElection[CatsEval](p1, state)
    val execution = new Execution()

    // when
    le(Cmd.coordinator.Elected("p2")).foldMap(EvalInterpreter(execution, eventMapper))
    le(Timeout(Leader)).foldMap(EvalInterpreter(execution, eventMapper))

    // then
    execution.print()

    execution.trace shouldBe Seq(
      Message(Timeout(io.parapet.core.processes.LeaderElection.Leader), p1)
    )

    state.waitForLeader shouldBe false

  }

  test("a node that received announce", Lemma2) {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"

    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val server = ProcessRef("server")
    val coordinator = ProcessRef("coordinator")

    val state = new State("p1", p1Addr, server, createPeers(Map(p2Addr -> p2)), coordinator)
    updatePeers(state)

    val le = new LeaderElection[CatsEval](p1, state)
    val execution = new Execution()

    // when
    le(Announce(p1Addr)).foldMap(EvalInterpreter(execution, eventMapper))
    le.sendHeartbeat.foldMap(EvalInterpreter(execution, eventMapper))

    // then
    execution.print()
    execution.trace.size shouldBe 3
    execution.trace.head should matchPattern {
      case Message(Req("p1", data), ProcessRef("parapet-noop")) if Cmd(data) == LeaderUpdate("p1", "p1:5555") => ()
    }
    execution.trace(1) should matchPattern {
      case Message(Req("p1", data), ProcessRef("p2")) if Cmd(data) == LeaderUpdate("p1", "p1:5555") => ()
    }
    execution.trace(2) shouldBe Message(Heartbeat(p1Addr, Option(p1Addr)), p2)

  }

  test("a leader crashed and cluster is complete", Lemma3) {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p3Addr = "p3:7777"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val p3 = ProcessRef("p3")
    val server = ProcessRef("server")
    val coordinator = ProcessRef("coordinator")

    val peerHeartbeatTimeout = 1.second
    val clock = new Clock.Mock(1.second)
    val state = new State("p1", p1Addr, server,
      createPeers(Map(p2Addr -> p2, p3Addr -> p3), peerHeartbeatTimeout, clock), coordinator)

    // todo separate test case
    state.peers.alive.map(p => p.address -> p.ref).toMap shouldBe Map(p2Addr -> p2, p3Addr -> p3)

    val le = new LeaderElection[CatsEval](p1, state)
    val execution = new Execution()

    // when
    le(Heartbeat(p2Addr, Option(p2Addr))).foldMap(EvalInterpreter(execution, eventMapper))
    state.leader shouldBe Some(p2Addr) // todo separate test case
    state.hasLeader shouldBe true // todo separate test case
    clock.tick(2.second)
    state.leader shouldBe Some(p2Addr)
    state.hasLeader shouldBe false
    state.peers.get(p3Addr).update(clock.currentTimeMillis)
    le.monitorCluster.foldMap(EvalInterpreter(execution, eventMapper))

    // then
    execution.print()
    state.peers.alive.map(p => p.address -> p.ref).toMap shouldBe Map(p3Addr -> p3)
    state.leader shouldBe Option.empty
    execution.trace shouldBe Seq(
      Message(Begin, p1)
    )
  }

  test("a leader crashed and cluster is not complete") {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p3Addr = "p3:7777"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val p3 = ProcessRef("p3")
    val server = ProcessRef("server")
    val coordinator = ProcessRef("coordinator")

    val peerHeartbeatTimeout = 1.second
    val clock = new Clock.Mock(1.second)
    val state = new State("p1", p1Addr, server,
      createPeers(Map(p2Addr -> p2, p3Addr -> p3), peerHeartbeatTimeout, clock), coordinator)

    // todo separate test case
    state.peers.alive.map(p => p.address -> p.ref).toMap shouldBe Map(p2Addr -> p2, p3Addr -> p3)

    val le = new LeaderElection[CatsEval](p1, state)
    val execution = new Execution()

    // when
    le(Heartbeat(p2Addr, Option(p2Addr))).foldMap(EvalInterpreter(execution, eventMapper))
    state.leader shouldBe Some(p2Addr) // todo separate test case
    state.hasLeader shouldBe true // todo separate test case
    clock.tick(2.second)
    state.leader shouldBe Some(p2Addr)
    state.hasLeader shouldBe false
    le.monitorCluster.foldMap(EvalInterpreter(execution, eventMapper))

    // then
    execution.print()
    state.peers.alive shouldBe Vector.empty
    state.leader shouldBe Option.empty
    execution.trace shouldBe Seq.empty
  }

  test("a node joins complete cluster with active leader and receives heartbeat from leader", Lemma4) {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p3Addr = "p2:7777"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val p3 = ProcessRef("p3")
    val server = ProcessRef("server")
    val coordinator = ProcessRef("coordinator")

    val peerHeartbeatTimeout = 1.second
    val clock = new Clock.Mock(1.second)
    val state = new State("p1", p1Addr, server,
      createPeers(Map(p2Addr -> p2, p3Addr -> p3), peerHeartbeatTimeout, clock), coordinator)

    val le = new LeaderElection[CatsEval](p1, state)
    val execution = new Execution()

    // when
    le(Heartbeat(p3Addr, Some(p3Addr))).foldMap(EvalInterpreter(execution, eventMapper))

    // then
    execution.print()
    state.peers.alive.map(p => p.address -> p.ref).toMap shouldBe Map(p2Addr -> p2, p3Addr -> p3)
    state.leader shouldBe Some(p3Addr)
    execution.trace shouldBe Seq.empty
  }

  test("a node joins incomplete cluster with active leader and receives heartbeat from leader") {
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
    val coordinator = ProcessRef("coordinator")
    val peerHeartbeatTimeout = 1.second
    val clock = new Clock.Mock(1.second)
    val state = new State("p1", p1Addr, server,
      createPeers(Map(p2Addr -> p2, p3Addr -> p3, p4Addr -> p4), peerHeartbeatTimeout, clock), coordinator)

    val le = new LeaderElection[CatsEval](p1, state)
    val execution = new Execution()

    // when
    clock.tick(1.second)
    le(Heartbeat(p3Addr, Some(p3Addr))).foldMap(EvalInterpreter(execution, eventMapper))

    // then
    execution.print()
    state.peers.alive.map(p => p.address -> p.ref).toMap shouldBe Map(p3Addr -> p3)
    state.leader shouldBe Option.empty
    execution.trace shouldBe Seq.empty
  }

  test("a node joins complete cluster with active leader and receives heartbeat from non leader") {
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
    val coordinator = ProcessRef("coordinator")
    val peerHeartbeatTimeout = 1.second
    val clock = new Clock.Mock(1.second)
    val state = new State("p1", p1Addr, server,
      createPeers(Map(p2Addr -> p2, p3Addr -> p3, p4Addr -> p4), peerHeartbeatTimeout, clock), coordinator)

    val le = new LeaderElection[CatsEval](p1, state)
    val execution = new Execution()

    // when
    clock.tick(1.second)
    state.peers.all.values.foreach(_.update(clock.currentTimeMillis))
    le(Heartbeat(p3Addr, Some(p2Addr))).foldMap(EvalInterpreter(execution, eventMapper))

    // then
    execution.print()
    state.peers.alive.map(p => p.address -> p.ref).toMap shouldBe Map(p2Addr -> p2, p3Addr -> p3, p4Addr -> p4)
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
    val coordinator = ProcessRef("coordinator")
    val state = new State("p1", p1Addr, server, createPeers(Map(p2Addr -> p2, p3Addr -> p3)), coordinator)
    state.leader(p2Addr)
    val le = new LeaderElection[CatsEval](p1, state)
    val execution = new Execution()

    // when
    assertThrows[IllegalStateException] {
      le(Heartbeat(p3Addr, Some(p3Addr))).foldMap(EvalInterpreter(execution, eventMapper))
    }

  }

  test("leader crashed and joined the cluster", Lemma5) {
    // given
    val p1Addr = "p1:5555"
    val p2Addr = "p2:6666"
    val p1 = ProcessRef("p1")
    val p2 = ProcessRef("p2")
    val server = ProcessRef("server")
    val coordinator = ProcessRef("coordinator")
    val state = new State("p1", p1Addr, server, createPeers(Map(p2Addr -> p2)), coordinator)
    state.leader(p2Addr)

    val execution = new Execution()
    val le = new LeaderElection[CatsEval](p1, state)

    // when
    le(Heartbeat(p2Addr, Option.empty)).foldMap(EvalInterpreter(execution, eventMapper))

    // then

    state.leader shouldBe Option.empty
  }


}

object LeaderElectionSpec {

  import io.parapet.core.doc.LeaderElectionDoc.Lemmas

  // @formatter:off
  object Lemma1 extends Tag(Lemmas.Lemma1.description)

  object Lemma2 extends Tag(Lemmas.Lemma2.description)

  object Lemma3 extends Tag(Lemmas.Lemma3.description)

  object Lemma4 extends Tag(Lemmas.Lemma4.description)

  object Lemma5 extends Tag(Lemmas.Lemma5.description)
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
      Peer.builder.id(p._2.value)
        .address(p._1)
        .ref(p._2)
        .timeoutMs(timeout)
        .clock(clock)
        .build).toVector)

}