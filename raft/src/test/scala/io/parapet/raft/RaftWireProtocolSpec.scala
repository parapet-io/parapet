package io.parapet.raft

import io.parapet.protocol.WireCodec.given
import io.parapet.protocol.raft.{RaftEnvelope => ProtoRaftEnvelope, VoteResponse => ProtoVoteResponse}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.*

class RaftWireProtocolSpec extends AnyFunSuite:
  test("round trips vote requests with group identity") {
    val message: RaftWireProtocol.RemoteMessage[String] = RaftWireProtocol.RemoteMessage.VoteRequest(
      RaftEvents.RequestVote(
        term = 7,
        candidateId = NodeId("node-a"),
        lastLogIndex = 4,
        lastLogTerm = 6
      )
    )

    val payload = RaftWireProtocol.encode[String](GroupId("raft-group"), message)
    val decoded = RaftWireProtocol.decode[String](payload)

    decoded shouldBe Right(RaftWireProtocol.Decoded(GroupId("raft-group"), message))
  }

  test("round trips log replication entries with command payloads") {
    val message = RaftWireProtocol.RemoteMessage.LogRequest(
      RaftEvents.AppendEntries(
        term = 3,
        leaderId = NodeId("leader-1"),
        prevLogIndex = 2,
        prevLogTerm = 2,
        entries = Vector(
          LogEntry(index = 3, term = 3, command = "set x=1"),
          LogEntry(index = 4, term = 3, command = "set y=2")
        ),
        leaderCommit = 4
      )
    )

    val payload = RaftWireProtocol.encode[String](GroupId("raft-group"), message)
    val decoded = RaftWireProtocol.decode[String](payload)

    decoded shouldBe Right(RaftWireProtocol.Decoded(GroupId("raft-group"), message))
  }

  test("rejects unsupported protocol versions") {
    val payload =
      ProtoRaftEnvelope(
        protocolVersion = 99,
        groupId = "raft-group",
        message = ProtoRaftEnvelope.Message.VoteResponse(
          ProtoVoteResponse(
            voterId = "node-b",
            term = 9,
            granted = true
          )
        )
      ).toByteArray

    RaftWireProtocol.decode[String](payload).left.toOption.getOrElse("") should include(
      "unsupported raft protocol version 99"
    )
  }
