package io.parapet.raft

import io.parapet.ProcessRef

import scala.concurrent.duration.*

/** The role a [[RaftNode]] currently plays in its consensus group.
  *
  * Roles transition per the Raft paper: every node starts as a [[Follower]], may become
  * a [[Candidate]] when an election timeout elapses, and is promoted to [[Leader]] upon
  * winning a majority vote.
  */
enum RaftRole derives CanEqual:
  /** Passive role; replays entries from the leader and votes in elections. */
  case Follower
  /** Soliciting votes after an election timeout fires. */
  case Candidate
  /** Authoritative replica; replicates entries to followers and advances the commit index. */
  case Leader

/** Identifier of a Raft consensus group; multiple groups may share a process. */
final case class GroupId(value: String) extends AnyVal

/** Identifier of a single Raft node within its [[GroupId]]. */
final case class NodeId(value: String) extends AnyVal

/** A single entry in the replicated log.
  *
  * @param index   1-based position in the log.
  * @param term    leader term in which the entry was created — used for safety checks.
  * @param command application command to be applied to the state machine on commit.
  */
final case class LogEntry[Command](index: Int, term: Long, command: Command):
  /** Alias for [[command]]; kept for backwards compatibility with older call sites. */
  def msg: Command = command

/** Per-node Raft configuration.
  *
  * @param groupId           consensus group this node participates in.
  * @param nodeId            this node's id within the group.
  * @param peers             remote peers, mapped to the local [[ProcessRef]] used to
  *                          deliver messages to them (typically a transport adapter).
  * @param observer          optional observer process notified about role changes and
  *                          committed commands — useful for tests, dashboards, and
  *                          state-machine replication.
  * @param electionTimeout   how long a follower waits without hearing from the leader
  *                          before starting a new election. Should be much larger than
  *                          [[heartbeatInterval]] and randomized in production deployments.
  * @param heartbeatInterval how often the leader emits empty AppendEntries to maintain
  *                          authority.
  */
final case class RaftConfig(
    groupId: GroupId,
    nodeId: NodeId,
    peers: Map[NodeId, ProcessRef],
    observer: Option[ProcessRef] = None,
    electionTimeout: FiniteDuration = 300.millis,
    heartbeatInterval: FiniteDuration = 100.millis
):
  /** Number of nodes that must agree for a quorum (strict majority of peers + self). */
  val quorumSize: Int =
    (peers.size + 1) / 2 + 1

/** Read-only snapshot of a [[RaftNode]]'s key state, suitable for status responses. */
final case class RaftSnapshot[State](
    groupId: GroupId,
    nodeId: NodeId,
    role: RaftRole,
    currentTerm: Long,
    leaderId: Option[NodeId],
    commitIndex: Int,
    lastApplied: Int,
    lastLogIndex: Int,
    state: State
):
  /** Alias for [[role]]. */
  def currentRole: RaftRole = role
  /** Alias for [[leaderId]]. */
  def currentLeader: Option[NodeId] = leaderId
  /** Alias for [[commitIndex]]. */
  def commitLength: Int = commitIndex
  /** Alias for [[lastLogIndex]]. */
  def logLength: Int = lastLogIndex
