package io.parapet.raft

import io.parapet.ProcessRef

/** Mutable bag of variables held by a [[RaftNode]].
  *
  * Kept as an internal mutable record (rather than a persistent case class) so the single-threaded handler in
  * [[RaftNode]] can update fields cheaply without allocating new state on every transition.
  *
  * The state mirrors the variables enumerated in the Raft paper - current term and vote, the replicated `log`,
  * commit/apply indexes, and per-peer replication cursors (`sentLength` / `ackedLength`). `pendingReplies` keeps client
  * `replyTo` refs alive across rounds until the corresponding entry is committed.
  */
final private[raft] class RaftNodeState[Command, State](initialState: State):
  var currentRole: RaftRole                = RaftRole.Follower
  var currentTerm: Long                    = 0L
  var votedFor: Option[NodeId]             = None
  var currentLeader: Option[NodeId]        = None
  var log: Vector[LogEntry[Command]]       = Vector.empty
  var commitLength: Int                    = 0
  var lastApplied: Int                     = 0
  var replicatedState: State               = initialState
  var votesReceived: Set[NodeId]           = Set.empty
  var sentLength: Map[NodeId, Int]         = Map.empty
  var ackedLength: Map[NodeId, Int]        = Map.empty
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
