package io.parapet.raft

import io.parapet.ProcessRef

import scala.concurrent.duration.*

enum RaftRole derives CanEqual:
  case Follower
  case Candidate
  case Leader

final case class GroupId(value: String) extends AnyVal
final case class NodeId(value: String) extends AnyVal

final case class LogEntry[Command](index: Int, term: Long, command: Command):
  def msg: Command = command

final case class RaftConfig(
    groupId: GroupId,
    nodeId: NodeId,
    peers: Map[NodeId, ProcessRef],
    observer: Option[ProcessRef] = None,
    electionTimeout: FiniteDuration = 300.millis,
    heartbeatInterval: FiniteDuration = 100.millis
):
  val quorumSize: Int =
    (peers.size + 1) / 2 + 1

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
  def currentRole: RaftRole = role
  def currentLeader: Option[NodeId] = leaderId
  def commitLength: Int = commitIndex
  def logLength: Int = lastLogIndex
