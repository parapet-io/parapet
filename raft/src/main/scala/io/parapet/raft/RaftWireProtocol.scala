package io.parapet.raft

import com.google.protobuf.ByteString
import io.parapet.protocol.WireCodec
import io.parapet.protocol.raft.{
  LogRequest => ProtoLogRequest,
  LogResponse => ProtoLogResponse,
  RaftEnvelope => ProtoRaftEnvelope,
  RaftLogEntry => ProtoRaftLogEntry,
  VoteRequest => ProtoVoteRequest,
  VoteResponse => ProtoVoteResponse
}
import io.parapet.raft.RaftEvents.{AppendEntries, AppendEntriesResponse, RequestVote, RequestVoteResponse}

import scala.util.Try

object RaftWireProtocol:
  final val CurrentProtocolVersion = 1

  enum RemoteMessage[Command]:
    case VoteRequest(value: RequestVote)
    case VoteResponse(value: RequestVoteResponse)
    case LogRequest(value: AppendEntries[Command])
    case LogResponse(value: AppendEntriesResponse)

  final case class Decoded[Command](
      groupId: GroupId,
      message: RemoteMessage[Command]
  )

  def encode[Command](
      groupId: GroupId,
      message: RemoteMessage[Command]
  )(using commandCodec: WireCodec[Command]): Array[Byte] =
    val envelope =
      message match
        case RemoteMessage.VoteRequest(value) =>
          ProtoRaftEnvelope(
            protocolVersion = CurrentProtocolVersion,
            groupId = groupId.value,
            message = ProtoRaftEnvelope.Message.VoteRequest(
              ProtoVoteRequest(
                candidateId = value.candidateId.value,
                term = value.term,
                lastLogIndex = value.lastLogIndex,
                lastLogTerm = value.lastLogTerm
              )
            )
          )
        case RemoteMessage.VoteResponse(value) =>
          ProtoRaftEnvelope(
            protocolVersion = CurrentProtocolVersion,
            groupId = groupId.value,
            message = ProtoRaftEnvelope.Message.VoteResponse(
              ProtoVoteResponse(
                voterId = value.voterId.value,
                term = value.term,
                granted = value.granted
              )
            )
          )
        case RemoteMessage.LogRequest(value) =>
          ProtoRaftEnvelope(
            protocolVersion = CurrentProtocolVersion,
            groupId = groupId.value,
            message = ProtoRaftEnvelope.Message.LogRequest(
              ProtoLogRequest(
                leaderId = value.leaderId.value,
                term = value.term,
                prefixLen = value.prevLogIndex,
                prefixTerm = value.prevLogTerm,
                leaderCommit = value.leaderCommit,
                suffix = value.entries.map(entryToProto)
              )
            )
          )
        case RemoteMessage.LogResponse(value) =>
          ProtoRaftEnvelope(
            protocolVersion = CurrentProtocolVersion,
            groupId = groupId.value,
            message = ProtoRaftEnvelope.Message.LogResponse(
              ProtoLogResponse(
                followerId = value.followerId.value,
                term = value.term,
                ack = value.matchIndex,
                success = value.success
              )
            )
          )

    envelope.toByteArray

  def decode[Command](bytes: Array[Byte])(using commandCodec: WireCodec[Command]): Either[String, Decoded[Command]] =
    Try(ProtoRaftEnvelope.parseFrom(bytes)).toEither.left.map(error => s"invalid raft payload: ${error.getMessage}").flatMap {
      envelope =>
        if envelope.protocolVersion != CurrentProtocolVersion then
          Left(
            s"unsupported raft protocol version ${envelope.protocolVersion}, expected $CurrentProtocolVersion"
          )
        else
          envelope.message match
            case ProtoRaftEnvelope.Message.VoteRequest(value) =>
              Right(
                Decoded(
                  GroupId(envelope.groupId),
                  RemoteMessage.VoteRequest(
                    RequestVote(
                      term = value.term,
                      candidateId = NodeId(value.candidateId),
                      lastLogIndex = value.lastLogIndex,
                      lastLogTerm = value.lastLogTerm
                    )
                  )
                )
              )
            case ProtoRaftEnvelope.Message.VoteResponse(value) =>
              Right(
                Decoded(
                  GroupId(envelope.groupId),
                  RemoteMessage.VoteResponse(
                    RequestVoteResponse(
                      term = value.term,
                      voterId = NodeId(value.voterId),
                      granted = value.granted
                    )
                  )
                )
              )
            case ProtoRaftEnvelope.Message.LogRequest(value) =>
              entriesFromProto(value.suffix).map { entries =>
                Decoded(
                  GroupId(envelope.groupId),
                  RemoteMessage.LogRequest(
                    AppendEntries(
                      term = value.term,
                      leaderId = NodeId(value.leaderId),
                      prevLogIndex = value.prefixLen,
                      prevLogTerm = value.prefixTerm,
                      entries = entries,
                      leaderCommit = value.leaderCommit
                    )
                  )
                )
              }
            case ProtoRaftEnvelope.Message.LogResponse(value) =>
              Right(
                Decoded(
                  GroupId(envelope.groupId),
                  RemoteMessage.LogResponse(
                    AppendEntriesResponse(
                      term = value.term,
                      followerId = NodeId(value.followerId),
                      success = value.success,
                      matchIndex = value.ack
                    )
                  )
                )
              )
            case ProtoRaftEnvelope.Message.Empty =>
              Left("raft envelope did not contain a message")
    }

  private def entryToProto[Command](entry: LogEntry[Command])(using commandCodec: WireCodec[Command]): ProtoRaftLogEntry =
    ProtoRaftLogEntry(
      index = entry.index,
      term = entry.term,
      command = ByteString.copyFrom(commandCodec.encode(entry.command))
    )

  private def entriesFromProto[Command](
      entries: Seq[ProtoRaftLogEntry]
  )(using commandCodec: WireCodec[Command]): Either[String, Vector[LogEntry[Command]]] =
    entries.foldLeft(Right(Vector.empty): Either[String, Vector[LogEntry[Command]]]) { (acc, entry) =>
      for
        decoded <- acc
        command <- commandCodec
          .decode(entry.command.toByteArray)
          .left
          .map(error => s"log entry ${entry.index} failed to decode: $error")
      yield decoded :+ LogEntry(entry.index, entry.term, command)
    }
