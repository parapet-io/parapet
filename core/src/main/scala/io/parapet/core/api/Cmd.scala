package io.parapet.core.api

import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, AvroSchema}
import io.parapet.{Event, ProcessRef}
import org.apache.avro.Schema

import java.io.ByteArrayOutputStream

/** Wire-level command algebra shared between the parapet `net` transports and the cluster coordination layer.
  *
  * Every concrete `Cmd` is also an [[Event]] so it can flow through the parapet runtime unchanged. Avro is used as the
  * on-the-wire codec - see [[Cmd.toByteArray]] / [[Cmd.apply]] for the round-trip.
  *
  * The grouped sub-objects ([[Cmd.netClient]], [[Cmd.netServer]], [[Cmd.cluster]], [[Cmd.clusterNode]]) act as
  * namespaces giving short, ergonomic names to the verbose fully qualified types.
  */
sealed trait Cmd extends Event:
  /** Serializes this command to a binary Avro buffer. The matching decoder is [[Cmd.apply]]. */
  def toByteArray: Array[Byte] =
    val outputStream = new ByteArrayOutputStream()
    val avroOutput   = AvroOutputStream.binary[Cmd].to(outputStream).build()
    avroOutput.write(this)
    avroOutput.flush()
    avroOutput.close()
    outputStream.toByteArray

/** Outbound payload for the network client; `reply`, when present, names the local process to deliver the eventual
  * response to.
  */
final case class NetClientSend(data: Array[Byte], reply: Option[ProcessRef] = None) extends Cmd

/** Reply returned to the original sender of a [[NetClientSend]]; `data` is `None` on timeout or transport failure.
  */
final case class NetClientRep(data: Option[Array[Byte]]) extends Cmd

/** Server-side outbound: send `data` to the connected client identified by `clientId`. */
final case class NetServerSend(clientId: String, data: Array[Byte]) extends Cmd

/** Server-side inbound: a message received from `clientId`. */
final case class NetServerMessage(clientId: String, data: Array[Byte]) extends Cmd

/** Status codes used in cluster control replies. */
sealed trait ClusterCode
object ClusterCode:
  /** Operation succeeded. */
  case object Ok extends ClusterCode

  /** Requested resource (node id, etc.) does not exist in the cluster. */
  case object NotFound extends ClusterCode

  /** Generic operation failure. */
  case object Error extends ClusterCode

  /** Node successfully joined the cluster. */
  case object Joined extends ClusterCode

  /** Notification that the gossiped cluster state was updated. */
  case object StateUpdate extends ClusterCode

  /** Initial handshake accepted by the peer. */
  case object HandshakeOk extends ClusterCode

/** Request from a node to join the cluster, advertising its address and logical group. */
final case class ClusterJoin(nodeId: String, address: String, group: String) extends Cmd

/** Reply to [[ClusterJoin]] indicating success or failure. */
final case class ClusterJoinResult(nodeId: String, code: ClusterCode) extends Cmd

/** Request to leave the cluster. */
final case class ClusterLeave(nodeId: String) extends Cmd

/** Lookup request for a single node's metadata. `senderId` identifies the caller. */
final case class ClusterGetNodeInfo(senderId: String, id: String) extends Cmd

/** Reply carrying a node's address (or [[ClusterCode.NotFound]] if unknown). */
final case class ClusterNodeInfo(id: String, address: String, code: ClusterCode) extends Cmd

/** Generic acknowledgement message used by various cluster RPCs. */
final case class ClusterAck(msg: String, code: ClusterCode) extends Cmd

/** A single entry in the cluster state - represents one peer node. */
final case class ClusterNodeEntry(id: String, protocol: String, address: String, groups: Set[String]) extends Cmd

/** Snapshot of the entire cluster state at version `version`. */
final case class ClusterState(version: Long, nodes: List[ClusterNodeEntry]) extends Cmd

/** Initial handshake message exchanged when a TCP connection is established. */
case object ClusterHandshake extends Cmd

/** Request a copy of the current [[ClusterState]] from a peer. */
case object ClusterGetState extends Cmd

/** Diagnostic command asking the receiver to log its cluster state. */
case object ClusterPrintState extends Cmd

/** Generic node-to-node request envelope; the inner `data` is opaque to the cluster layer and decoded by application
  * code.
  */
final case class ClusterNodeReq(nodeId: String, data: Array[Byte]) extends Cmd

/** Union of every command the network client process exchanges. */
type NetClientApi = NetClientSend | NetClientRep

/** Union of every command the network server process exchanges. */
type NetServerApi = NetServerSend | NetServerMessage

/** Union of every command in the cluster control protocol. */
type ClusterApi =
  ClusterJoin | ClusterJoinResult | ClusterLeave | ClusterGetNodeInfo | ClusterNodeInfo | ClusterAck |
    ClusterNodeEntry | ClusterState | ClusterHandshake.type | ClusterGetState.type | ClusterPrintState.type

/** Union of cluster node-to-node application requests. */
type ClusterNodeApi = ClusterNodeReq

/** Companion holding the Avro schema, the binary decoder, and the namespaced ergonomic aliases for each sub-protocol.
  */
object Cmd:
  private[api] val schema: Schema = AvroSchema[Cmd]

  /** Aliases for the network-client sub-protocol. */
  object netClient:
    type Api  = NetClientApi
    type Send = NetClientSend
    val Send = NetClientSend
    type Rep = NetClientRep
    val Rep = NetClientRep

  /** Aliases for the network-server sub-protocol. */
  object netServer:
    type Api  = NetServerApi
    type Send = NetServerSend
    val Send = NetServerSend
    type Message = NetServerMessage
    val Message = NetServerMessage

  /** Aliases for the cluster control sub-protocol. */
  object cluster:
    type Api = ClusterApi

    type Code = ClusterCode
    object Code:
      val Ok: ClusterCode          = ClusterCode.Ok
      val NotFound: ClusterCode    = ClusterCode.NotFound
      val Error: ClusterCode       = ClusterCode.Error
      val Joined: ClusterCode      = ClusterCode.Joined
      val StateUpdate: ClusterCode = ClusterCode.StateUpdate
      val HandshakeOk: ClusterCode = ClusterCode.HandshakeOk

    type Join = ClusterJoin
    val Join = ClusterJoin
    type JoinResult = ClusterJoinResult
    val JoinResult = ClusterJoinResult
    type Leave = ClusterLeave
    val Leave = ClusterLeave
    type GetNodeInfo = ClusterGetNodeInfo
    val GetNodeInfo = ClusterGetNodeInfo
    type NodeInfo = ClusterNodeInfo
    val NodeInfo = ClusterNodeInfo
    type Ack = ClusterAck
    val Ack = ClusterAck
    type Node = ClusterNodeEntry
    val Node = ClusterNodeEntry
    type State = ClusterState
    val State = ClusterState
    type Handshake = ClusterHandshake.type
    val Handshake: ClusterHandshake.type = ClusterHandshake
    type GetState = ClusterGetState.type
    val GetState: ClusterGetState.type = ClusterGetState
    type PrintState = ClusterPrintState.type
    val PrintState: ClusterPrintState.type = ClusterPrintState

  /** Aliases for the cluster node-to-node sub-protocol. */
  object clusterNode:
    type Api = ClusterNodeApi
    type Req = ClusterNodeReq
    val Req = ClusterNodeReq

  /** Inverse of [[Cmd.toByteArray]]: decodes a single [[Cmd]] from its Avro binary form. */
  def apply(bytes: Array[Byte]): Cmd =
    val avroInput = AvroInputStream.binary[Cmd].from(bytes).build(schema)
    avroInput.iterator.toSet.head
