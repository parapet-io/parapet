package io.parapet.core.api

import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, AvroSchema}
import io.parapet.{Event, ProcessRef}
import org.apache.avro.Schema

import java.io.ByteArrayOutputStream

sealed trait Cmd extends Event:
  def toByteArray: Array[Byte] =
    val outputStream = new ByteArrayOutputStream()
    val avroOutput = AvroOutputStream.binary[Cmd].to(outputStream).build()
    avroOutput.write(this)
    avroOutput.flush()
    avroOutput.close()
    outputStream.toByteArray

final case class NetClientSend(data: Array[Byte], reply: Option[ProcessRef] = None) extends Cmd
final case class NetClientRep(data: Option[Array[Byte]]) extends Cmd

final case class NetServerSend(clientId: String, data: Array[Byte]) extends Cmd
final case class NetServerMessage(clientId: String, data: Array[Byte]) extends Cmd

sealed trait ClusterCode
object ClusterCode:
  case object Ok extends ClusterCode
  case object NotFound extends ClusterCode
  case object Error extends ClusterCode
  case object Joined extends ClusterCode
  case object StateUpdate extends ClusterCode
  case object HandshakeOk extends ClusterCode

final case class ClusterJoin(nodeId: String, address: String, group: String) extends Cmd
final case class ClusterJoinResult(nodeId: String, code: ClusterCode) extends Cmd
final case class ClusterLeave(nodeId: String) extends Cmd
final case class ClusterGetNodeInfo(senderId: String, id: String) extends Cmd
final case class ClusterNodeInfo(id: String, address: String, code: ClusterCode) extends Cmd
final case class ClusterAck(msg: String, code: ClusterCode) extends Cmd
final case class ClusterNodeEntry(id: String, protocol: String, address: String, groups: Set[String]) extends Cmd
final case class ClusterState(version: Long, nodes: List[ClusterNodeEntry]) extends Cmd
case object ClusterHandshake extends Cmd
case object ClusterGetState extends Cmd
case object ClusterPrintState extends Cmd

final case class ClusterNodeReq(nodeId: String, data: Array[Byte]) extends Cmd

type NetClientApi = NetClientSend | NetClientRep
type NetServerApi = NetServerSend | NetServerMessage
type ClusterApi =
  ClusterJoin | ClusterJoinResult | ClusterLeave | ClusterGetNodeInfo | ClusterNodeInfo | ClusterAck |
    ClusterNodeEntry | ClusterState | ClusterHandshake.type | ClusterGetState.type | ClusterPrintState.type
type ClusterNodeApi = ClusterNodeReq

object Cmd:
  private[api] val schema: Schema = AvroSchema[Cmd]

  object netClient:
    type Api = NetClientApi
    type Send = NetClientSend
    val Send = NetClientSend
    type Rep = NetClientRep
    val Rep = NetClientRep

  object netServer:
    type Api = NetServerApi
    type Send = NetServerSend
    val Send = NetServerSend
    type Message = NetServerMessage
    val Message = NetServerMessage

  object cluster:
    type Api = ClusterApi

    type Code = ClusterCode
    object Code:
      val Ok: ClusterCode = ClusterCode.Ok
      val NotFound: ClusterCode = ClusterCode.NotFound
      val Error: ClusterCode = ClusterCode.Error
      val Joined: ClusterCode = ClusterCode.Joined
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

  object clusterNode:
    type Api = ClusterNodeApi
    type Req = ClusterNodeReq
    val Req = ClusterNodeReq

  def apply(bytes: Array[Byte]): Cmd =
    val avroInput = AvroInputStream.binary[Cmd].from(bytes).build(schema)
    avroInput.iterator.toSet.head
