package io.parapet.raft

import io.parapet.core.Events.Start
import io.parapet.core.Process
import io.parapet.raft.RaftEvents.*
import io.parapet.{Event, ProcessRef}

/** Single Raft replica implemented as a parapet [[Process]].
  *
  * Implements the canonical Raft consensus algorithm — leader election, log
  * replication, follower log update, and quorum commit advancement — organised
  * around section 6.2 of `dist-sys-notes.pdf`. Inbound RPCs are modelled as
  * [[RaftEvents.RaftEvent]]s; replies are delivered through `config.peers`. Clients
  * submit work via [[RaftEvents.ClientCommand]] and observe progress through
  * `config.observer` (see [[RaftEvents.RoleChanged]] / [[RaftEvents.CommandCommitted]]).
  *
  * The node is intentionally single-threaded: parapet's per-process inbox serialises
  * all events, so the mutable [[RaftNodeState]] held internally can be updated
  * without locking.
  *
  * @param config       node configuration (peers, timeouts, observer).
  * @param stateMachine pure command applier invoked once per committed entry.
  * @param initialState seed state passed to `stateMachine` for the first apply.
  * @param ref          optional address; defaults to a freshly-generated UUID ref so
  *                     multiple replicas can coexist in the same runtime.
  */
class RaftNode[F[_], Command, State](
    val config: RaftConfig,
    stateMachine: RaftStateMachine[State, Command],
    initialState: State,
    override val ref: ProcessRef = ProcessRef.jdkUUIDRef
) extends Process[F]:

  import dsl.*

  override val name: String = s"raft-${config.groupId.value}-${config.nodeId.value}"

  private val node = new RaftNodeState[Command, State](initialState)

  /** Return a read-only [[RaftSnapshot]] of this replica's current state. */
  def snapshot: RaftSnapshot[State] =
    node.snapshot(config)

  override def handle: Receive =
    {
      case Start =>
        scheduleElectionTimeout(node.currentTerm)

      case ElectionTimeout(term) if term == node.currentTerm && node.currentRole != RaftRole.Leader =>
        startLeaderElection

      case HeartbeatTimeout(term) if term == node.currentTerm && node.currentRole == RaftRole.Leader =>
        replicateLogToFollowers ++ scheduleHeartbeat(node.currentTerm)

      case RequestVote(term, candidateId, candidateLastIndex, candidateLastTerm) =>
        receiveVoteRequest(term, candidateId, candidateLastIndex, candidateLastTerm)

      case RequestVoteResponse(term, voterId, granted) =>
        receiveVoteResponse(term, voterId, granted)

      case AppendEntries(term, remoteLeaderId, prefixLen, prefixTerm, entries, leaderCommit) =>
        receiveLogRequest(
          term,
          remoteLeaderId,
          prefixLen,
          prefixTerm,
          entries.asInstanceOf[Vector[LogEntry[Command]]],
          leaderCommit
        )

      case AppendEntriesResponse(term, followerId, success, ack) =>
        receiveLogResponse(term, followerId, success, ack)

      case ClientCommand(command, replyTo) =>
        broadcastCommand(command.asInstanceOf[Command], replyTo)

      case StatusRequest(replyTo) =>
        StatusResponse(snapshot) ~> replyTo
    }

  // Slides 111-114: election and vote collection.
  private def startLeaderElection: Program =
    eval {
      node.currentTerm = node.currentTerm + 1
      node.currentRole = RaftRole.Candidate
      node.votedFor = Some(config.nodeId)
      node.currentLeader = None
      node.votesReceived = Set(config.nodeId)
    } ++ emitRoleChanged ++ scheduleElectionTimeout(node.currentTerm) ++
      (if config.peers.isEmpty then becomeLeader
       else sendToAllPeers {
         RequestVote(node.currentTerm, config.nodeId, node.lastLogIndex, node.lastLogTerm)
       })

  private def receiveVoteRequest(
      term: Long,
      candidateId: NodeId,
      candidateLastIndex: Int,
      candidateLastTerm: Long
  ): Program =
    flow {
      if term < node.currentTerm then
        replyToPeer(candidateId, RequestVoteResponse(node.currentTerm, config.nodeId, granted = false))
      else
        val steppedDown =
          if term > node.currentTerm || node.currentRole != RaftRole.Follower then
            stepDown(term, None)
          else
            unit

        steppedDown ++ eval {
          val canVote = node.votedFor.isEmpty || node.votedFor.contains(candidateId)
          val upToDate = isCandidateUpToDate(candidateLastIndex, candidateLastTerm)
          if canVote && upToDate then
            node.votedFor = Some(candidateId)
            true
          else
            false
        }.flatMap { granted =>
          replyToPeer(candidateId, RequestVoteResponse(node.currentTerm, config.nodeId, granted)) ++
            (if granted then scheduleElectionTimeout(node.currentTerm) else unit)
        }
    }

  private def receiveVoteResponse(term: Long, voterId: NodeId, granted: Boolean): Program =
    flow {
      if node.currentRole != RaftRole.Candidate then
        unit
      else if term > node.currentTerm then
        stepDown(term, None)
      else if term == node.currentTerm && granted then
        eval {
          node.votesReceived = node.votesReceived + voterId
        } ++
          (if node.votesReceived.size >= config.quorumSize then becomeLeader else unit)
      else
        unit
    }

  // Slides 115-119: broadcast, replication, and follower/leader log handling.
  private def receiveLogRequest(
      term: Long,
      remoteLeaderId: NodeId,
      prefixLen: Int,
      prefixTerm: Long,
      suffix: Vector[LogEntry[Command]],
      leaderCommitLength: Int
  ): Program =
    flow {
      if term < node.currentTerm then
        replyToPeer(remoteLeaderId, AppendEntriesResponse(node.currentTerm, config.nodeId, success = false, 0))
      else
        val steppedDown =
          if term > node.currentTerm || node.currentRole != RaftRole.Follower || node.currentLeader != Some(remoteLeaderId) then
            stepDown(term, Some(remoteLeaderId))
          else
            unit

        steppedDown ++
          (if logMatches(prefixLen, prefixTerm) then
             eval {
               appendLogEntries(prefixLen, leaderCommitLength, suffix)
               node.currentLeader = Some(remoteLeaderId)
             } ++
               applyCommittedEntries ++
               replyToPeer(
                 remoteLeaderId,
                 AppendEntriesResponse(node.currentTerm, config.nodeId, success = true, prefixLen + suffix.length)
               ) ++
               scheduleElectionTimeout(node.currentTerm)
           else
             replyToPeer(remoteLeaderId, AppendEntriesResponse(node.currentTerm, config.nodeId, success = false, 0)))
    }

  private def receiveLogResponse(term: Long, followerId: NodeId, success: Boolean, ack: Int): Program =
    flow {
      if node.currentRole != RaftRole.Leader then
        unit
      else if term > node.currentTerm then
        stepDown(term, None)
      else if success then
        eval {
          node.sentLength = node.sentLength.updated(followerId, ack)
          node.ackedLength = node.ackedLength.updated(followerId, ack)
        } ++ commitReadyEntries
      else
        eval {
          val current = node.sentLength.getOrElse(followerId, node.lastLogIndex)
          node.sentLength = node.sentLength.updated(followerId, math.max(0, current - 1))
        } ++ replicateLog(followerId)
    }

  private def broadcastCommand(command: Command, replyTo: Option[ProcessRef]): Program =
    flow {
      if node.currentRole != RaftRole.Leader then
        replyMaybe(replyTo, NotLeader(config.groupId, config.nodeId, node.currentLeader))
      else
        eval {
          val entry = LogEntry(node.lastLogIndex + 1, node.currentTerm, command)
          node.log = node.log :+ entry
          replyTo.foreach(target => node.pendingReplies = node.pendingReplies.updated(entry.index, target))
          node.sentLength = node.sentLength.updated(config.nodeId, entry.index)
          node.ackedLength = node.ackedLength.updated(config.nodeId, entry.index)
        } ++
          (if config.peers.isEmpty then
             eval {
               node.commitLength = node.lastLogIndex
             } ++ applyCommittedEntries
           else
             replicateLogToFollowers)
    }

  private def becomeLeader: Program =
    eval {
      node.currentRole = RaftRole.Leader
      node.currentLeader = Some(config.nodeId)
      node.votesReceived = Set.empty
      node.votedFor = Some(config.nodeId)
      val logLength = node.lastLogIndex
      node.sentLength = config.peers.keys.map(_ -> logLength).toMap.updated(config.nodeId, logLength)
      node.ackedLength = config.peers.keys.map(_ -> 0).toMap.updated(config.nodeId, logLength)
    } ++ emitRoleChanged ++ replicateLogToFollowers ++ scheduleHeartbeat(node.currentTerm)

  private def stepDown(term: Long, newLeader: Option[NodeId]): Program =
    eval {
      node.currentTerm = term
      node.currentRole = RaftRole.Follower
      node.votedFor = None
      node.votesReceived = Set.empty
      node.currentLeader = newLeader
      node.sentLength = Map.empty
      node.ackedLength = Map.empty
    } ++ emitRoleChanged ++ scheduleElectionTimeout(node.currentTerm)

  private def replicateLogToFollowers: Program =
    config.peers.keys.foldLeft(unit) { (acc, nodeId) =>
      acc ++ replicateLog(nodeId)
    }

  private def replicateLog(nodeId: NodeId): Program =
    config.peers.get(nodeId) match
      case Some(target) =>
        val prefixLen = node.sentLength.getOrElse(nodeId, node.lastLogIndex)
        val suffix = node.log.drop(prefixLen)
        AppendEntries(
          node.currentTerm,
          config.nodeId,
          prefixLen,
          node.termAt(prefixLen),
          suffix,
          node.commitLength
        ) ~> target
      case None =>
        unit

  private def sendToAllPeers(event: => Event): Program =
    config.peers.values.foldLeft(unit) { (acc, target) =>
      acc ++ (event ~> target)
    }

  // Slide 120: quorum commit advancement and delivery to the state machine.
  private def commitReadyEntries: Program =
    eval {
      val ready = (1 to node.lastLogIndex).filter(length => acks(length) >= config.quorumSize)
      ready.lastOption.foreach { length =>
        if length > node.commitLength && node.termAt(length) == node.currentTerm then
          node.commitLength = length
      }
    } ++ applyCommittedEntries

  private def applyCommittedEntries: Program =
    def loop: Program =
      if node.lastApplied >= node.commitLength then
        unit
      else
        eval {
          node.lastApplied = node.lastApplied + 1
          val entry = node.log(node.lastApplied - 1)
          node.replicatedState = stateMachine(node.replicatedState, entry.command)
          val replyTo = node.pendingReplies.get(entry.index)
          node.pendingReplies = node.pendingReplies.removed(entry.index)
          (entry.index, replyTo)
        }.flatMap { case (index, replyTo) =>
          emitCommitted(index, replyTo) ++ loop
        }

    loop

  private def appendLogEntries(prefixLen: Int, leaderCommitLength: Int, suffix: Vector[LogEntry[Command]]): Unit =
    if suffix.nonEmpty && node.log.length > prefixLen then
      val index = math.min(node.log.length, prefixLen + suffix.length) - 1
      if node.log(index).term != suffix(index - prefixLen).term then
        node.log = node.log.take(prefixLen)

    if prefixLen + suffix.length > node.log.length then
      val missing = suffix.drop(node.log.length - prefixLen)
      node.log = node.log ++ missing

    if leaderCommitLength > node.commitLength then
      node.commitLength = math.min(leaderCommitLength, node.lastLogIndex)

  private def isCandidateUpToDate(candidateLastIndex: Int, candidateLastTerm: Long): Boolean =
    candidateLastTerm > node.lastLogTerm ||
      (candidateLastTerm == node.lastLogTerm && candidateLastIndex >= node.lastLogIndex)

  private def logMatches(prefixLen: Int, prefixTerm: Long): Boolean =
    (node.log.length >= prefixLen) &&
      (prefixLen == 0 || node.log(prefixLen - 1).term == prefixTerm)

  private def acks(length: Int): Int =
    node.ackedLength.values.count(_ >= length)

  private def scheduleElectionTimeout(term: Long): Program =
    fork(delay(config.electionTimeout) ++ ElectionTimeout(term) ~> ref).void

  private def scheduleHeartbeat(term: Long): Program =
    fork(delay(config.heartbeatInterval) ++ HeartbeatTimeout(term) ~> ref).void

  private def replyToPeer(nodeId: NodeId, event: Event): Program =
    config.peers.get(nodeId) match
      case Some(target) => event ~> target
      case None         => unit

  private def replyMaybe(target: Option[ProcessRef], event: => Event): Program =
    target match
      case Some(value) => event ~> value
      case None        => unit

  private def emitRoleChanged: Program =
    config.observer match
      case Some(observer) =>
        RoleChanged(config.groupId, config.nodeId, node.currentRole, node.currentTerm, node.currentLeader) ~> observer
      case None =>
        unit

  private def emitCommitted(index: Int, replyTo: Option[ProcessRef]): Program =
    val committed = CommandCommitted(config.groupId, config.nodeId, index, node.currentTerm, node.replicatedState)
    replyMaybe(replyTo, committed) ++
      (config.observer match
        case Some(observer) => committed ~> observer
        case None           => unit)
