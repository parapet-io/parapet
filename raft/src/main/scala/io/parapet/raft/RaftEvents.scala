package io.parapet.raft

import io.parapet.{Event, ProcessRef}

/** Wire-level [[io.parapet.Event]] types exchanged between [[RaftNode]] instances and with
  * external clients.
  *
  * The event payloads mirror the Raft paper's RPCs ([[RequestVote]] / [[AppendEntries]])
  * plus parapet-specific control events ([[ElectionTimeout]], [[ClientCommand]],
  * [[StatusRequest]]).
  */
object RaftEvents:
  /** Marker for messages internal to a Raft consensus group. */
  sealed trait RaftEvent extends Event

  /** Self-scheduled timer telling the node it has waited long enough for the leader. */
  final case class ElectionTimeout(term: Long) extends RaftEvent

  /** Self-scheduled timer telling the leader to send the next heartbeat. */
  final case class HeartbeatTimeout(term: Long) extends RaftEvent

  /** Candidate → peer: solicit a vote in `term`.
    *
    * The recipient grants the vote only if (a) the candidate's log is at least as
    * up-to-date as its own and (b) it has not yet voted in this term.
    */
  final case class RequestVote(
      term: Long,
      candidateId: NodeId,
      lastLogIndex: Int,
      lastLogTerm: Long
  ) extends RaftEvent

  /** Peer → candidate: response to [[RequestVote]]. */
  final case class RequestVoteResponse(
      term: Long,
      voterId: NodeId,
      granted: Boolean
  ) extends RaftEvent

  /** Leader → follower: replicate log entries (`entries` may be empty for heartbeat).
    *
    * @param prevLogIndex index immediately before `entries`.
    * @param prevLogTerm  term of the entry at `prevLogIndex` — used for the consistency
    *                     check.
    * @param leaderCommit leader's current commit index, lets followers advance their own.
    */
  final case class AppendEntries[Command](
      term: Long,
      leaderId: NodeId,
      prevLogIndex: Int,
      prevLogTerm: Long,
      entries: Vector[LogEntry[Command]],
      leaderCommit: Int
  ) extends RaftEvent

  /** Follower → leader: response to [[AppendEntries]]. `matchIndex` is the highest log
    * index known to be replicated on the follower.
    */
  final case class AppendEntriesResponse(
      term: Long,
      followerId: NodeId,
      success: Boolean,
      matchIndex: Int
  ) extends RaftEvent

  /** Client → any node: submit `command` for commit. Non-leaders respond with a
    * [[NotLeader]] event so the client can retry against the current leader.
    *
    * @param replyTo optional ref to deliver the eventual [[CommandCommitted]] (or
    *                [[NotLeader]]) reply to.
    */
  final case class ClientCommand[Command](
      command: Command,
      replyTo: Option[ProcessRef] = None
  ) extends RaftEvent

  /** Client → node: request a [[StatusResponse]] snapshot. */
  final case class StatusRequest(replyTo: ProcessRef) extends RaftEvent

  /** Observer notification: the node transitioned to a new role. */
  final case class RoleChanged(
      groupId: GroupId,
      nodeId: NodeId,
      role: RaftRole,
      term: Long,
      leaderId: Option[NodeId]
  ) extends Event

  /** Reply to [[ClientCommand]] when the receiver isn't the leader; carries the leader
    * hint when known so the client can retry.
    */
  final case class NotLeader(
      groupId: GroupId,
      nodeId: NodeId,
      leaderId: Option[NodeId]
  ) extends Event

  /** Reply to [[ClientCommand]] / observer notification: a command at `index` was
    * committed and applied; `state` is the post-apply state machine snapshot.
    */
  final case class CommandCommitted[State](
      groupId: GroupId,
      nodeId: NodeId,
      index: Int,
      term: Long,
      state: State
  ) extends Event

  /** Reply to [[StatusRequest]]. */
  final case class StatusResponse[State](snapshot: RaftSnapshot[State]) extends Event
