package io.parapet.raft

import io.parapet.{Event, ProcessRef}

object RaftEvents:
  sealed trait RaftEvent extends Event

  final case class ElectionTimeout(term: Long) extends RaftEvent
  final case class HeartbeatTimeout(term: Long) extends RaftEvent

  final case class RequestVote(
      term: Long,
      candidateId: NodeId,
      lastLogIndex: Int,
      lastLogTerm: Long
  ) extends RaftEvent

  final case class RequestVoteResponse(
      term: Long,
      voterId: NodeId,
      granted: Boolean
  ) extends RaftEvent

  final case class AppendEntries[Command](
      term: Long,
      leaderId: NodeId,
      prevLogIndex: Int,
      prevLogTerm: Long,
      entries: Vector[LogEntry[Command]],
      leaderCommit: Int
  ) extends RaftEvent

  final case class AppendEntriesResponse(
      term: Long,
      followerId: NodeId,
      success: Boolean,
      matchIndex: Int
  ) extends RaftEvent

  final case class ClientCommand[Command](
      command: Command,
      replyTo: Option[ProcessRef] = None
  ) extends RaftEvent

  final case class StatusRequest(replyTo: ProcessRef) extends RaftEvent

  final case class RoleChanged(
      groupId: GroupId,
      nodeId: NodeId,
      role: RaftRole,
      term: Long,
      leaderId: Option[NodeId]
  ) extends Event

  final case class NotLeader(
      groupId: GroupId,
      nodeId: NodeId,
      leaderId: Option[NodeId]
  ) extends Event

  final case class CommandCommitted[State](
      groupId: GroupId,
      nodeId: NodeId,
      index: Int,
      term: Long,
      state: State
  ) extends Event

  final case class StatusResponse[State](snapshot: RaftSnapshot[State]) extends Event
