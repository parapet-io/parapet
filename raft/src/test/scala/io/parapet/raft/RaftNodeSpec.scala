package io.parapet.raft

import io.parapet.ProcessRef
import io.parapet.core.TestUtils.*
import io.parapet.core.TestUtils.given
import io.parapet.raft.RaftEvents.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

class RaftNodeSpec extends AnyFunSuite:
  private val machine =
    RaftStateMachine[Vector[String], String]((state, command) => state :+ command)

  test("follower grants vote to an up-to-date candidate") {
    val candidateId = NodeId("n2")
    val candidateRef = ProcessRef("n2")
    val observer = ProcessRef("observer")
    val node = new RaftNode[Id, String, Vector[String]](
      RaftConfig(GroupId("g1"), NodeId("n1"), Map(candidateId -> candidateRef), observer = Some(observer)),
      machine,
      Vector.empty,
      ref = ProcessRef("n1")
    )

    val execution = new Execution()
    node(RequestVote(term = 1, candidateId, lastLogIndex = 0, lastLogTerm = 0)).foldMap(new IdInterpreter(execution))

    node.snapshot.currentTerm shouldBe 1L
    execution.trace.toList.exists(message =>
      message.target == candidateRef &&
        message.event == RequestVoteResponse(1, NodeId("n1"), granted = true)
    ) shouldBe true
  }

  test("candidate becomes leader after majority votes") {
    val peerId = NodeId("n2")
    val peerRef = ProcessRef("n2")
    val observer = ProcessRef("observer")
    val node = new RaftNode[Id, String, Vector[String]](
      RaftConfig(GroupId("g1"), NodeId("n1"), Map(peerId -> peerRef), observer = Some(observer)),
      machine,
      Vector.empty,
      ref = ProcessRef("n1")
    )

    val execution = new Execution()
    val interpreter = new IdInterpreter(execution)

    node(ElectionTimeout(0)).foldMap(interpreter)
    node(RequestVoteResponse(1, peerId, granted = true)).foldMap(interpreter)

    node.snapshot.role shouldBe RaftRole.Leader
    node.snapshot.leaderId shouldBe Some(NodeId("n1"))
    execution.trace.toList.exists(_.event == RoleChanged(GroupId("g1"), NodeId("n1"), RaftRole.Leader, 1, Some(NodeId("n1")))) shouldBe true
  }

  test("leader commits a command immediately in a single-node group") {
    val replyTo = ProcessRef("client")
    val observer = ProcessRef("observer")
    val node = new RaftNode[Id, String, Vector[String]](
      RaftConfig(GroupId("g1"), NodeId("n1"), Map.empty, observer = Some(observer)),
      machine,
      Vector.empty,
      ref = ProcessRef("n1")
    )

    val execution = new Execution()
    val interpreter = new IdInterpreter(execution)

    node(ElectionTimeout(0)).foldMap(interpreter)
    node(ClientCommand("set-x", Some(replyTo))).foldMap(interpreter)

    node.snapshot.role shouldBe RaftRole.Leader
    node.snapshot.commitIndex shouldBe 1
    node.snapshot.state shouldBe Vector("set-x")
    execution.trace.toList.exists(_.target == replyTo) shouldBe true
  }
