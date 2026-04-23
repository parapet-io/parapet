package io.parapet.raft

import io.parapet.ProcessRef

private[raft] final class RaftNodeState[Command, State](initialState: State):
  var currentRole: RaftRole = RaftRole.Follower
  var currentTerm: Long = 0L
  var votedFor: Option[NodeId] = None
  var currentLeader: Option[NodeId] = None
  var log: Vector[LogEntry[Command]] = Vector.empty
  var commitLength: Int = 0
  var lastApplied: Int = 0
  var replicatedState: State = initialState
  var votesReceived: Set[NodeId] = Set.empty
  var sentLength: Map[NodeId, Int] = Map.empty
  var ackedLength: Map[NodeId, Int] = Map.empty
  var pendingReplies: Map[Int, ProcessRef] = Map.empty

  def lastLogIndex: Int =
    log.lastOption.map(_.index).getOrElse(0)

  def lastLogTerm: Long =
    log.lastOption.map(_.term).getOrElse(0L)

  def termAt(index: Int): Long =
    if index <= 0 then 0L else log(index - 1).term

  def snapshot(config: RaftConfig): RaftSnapshot[State] =
    RaftSnapshot(
      groupId = config.groupId,
      nodeId = config.nodeId,
      role = currentRole,
      currentTerm = currentTerm,
      leaderId = currentLeader,
      commitIndex = commitLength,
      lastApplied = lastApplied,
      lastLogIndex = lastLogIndex,
      state = replicatedState
    )
