package io.parapet.cluster.api

import io.parapet.core.{Encoder, Event}

import java.nio.ByteBuffer

object ClusterApi {


  // @formatter:off
  sealed trait API extends Event

  // --------------------- JOIN ------------------------------------------ //
  object JoinResultCodes {
    val OK = 0
    val ERROR = 1 // must to be clarified
  }

  // FORMAT
  val TAG_SIZE = 4
  val NODE_ID_SIZE = 4
  val ADDR_SIZE = 4
  val GROUP_SIZE = 4
  val RESULT_CODE_SIZE = 4

  case class Join(nodeId: String, address: String, group: String) extends API
  case class JoinResult(nodeId: String, code: Int) extends API

  // ---------------------- Result ---------------------------------------- //
  case class Result(code: Int, msg: String) extends API

  object ResultCodes {
    val OK = 0
    val ERROR = 1
  }
  // FORMAT
  val CODE_SIZE = 4
  val MSG_SIZE = 4

  // ---------------------- NodeInfo ---------------------------------------- //
  case class GetNodeInfo(senderId: String, id: String) extends API
  case class NodeInfo(address: String, code: Int) extends API
  object NodeInfoCodes {
    val OK = 0
    val NODE_NOT_FOUND = 1
    val ERROR = 2
  }

  // TAGS
  val JOIN_TAG = 20
  val JOIN_RESULT_TAG = 21
  val RESULT_TAG = 22
  val GET_NODE_INFO_TAG = 23
  val NODE_INFO = 24

  // @formatter:on

  val encoder: Encoder = new Encoder {
    override def write(e: Event): Array[Byte] = {
      e match {
        case Join(nodeId, address, group) =>
          val nodeIdBytes = nodeId.getBytes()
          val addressBytes = address.getBytes()
          val groupBytes = group.getBytes()
          val buf = ByteBuffer.allocate(TAG_SIZE +
            (NODE_ID_SIZE + nodeIdBytes.length) +
            (GROUP_SIZE + groupBytes.length) +
            (ADDR_SIZE + addressBytes.length))
          // write
          buf.putInt(JOIN_TAG)
          putWithSize(buf, nodeIdBytes)
          putWithSize(buf, addressBytes)
          putWithSize(buf, groupBytes)
          buf.rewind()
          buf.array()
        case JoinResult(nodeId, code) =>
          val nodeIdBytes = nodeId.getBytes()
          val buf = ByteBuffer.allocate(TAG_SIZE + (NODE_ID_SIZE + nodeIdBytes.length) + RESULT_CODE_SIZE)
          buf.putInt(JOIN_RESULT_TAG)
          putWithSize(buf, nodeIdBytes)
          buf.putInt(code)
          buf.rewind()
          buf.array()
        case Result(code, msg) =>
          val msgBytes = msg.getBytes()
          val buf = ByteBuffer.allocate(TAG_SIZE + CODE_SIZE + (MSG_SIZE + msgBytes.length))
          buf.putInt(RESULT_TAG)
          buf.putInt(code)
          putWithSize(buf, msgBytes)
          buf.rewind()
          buf.array()
        case GetNodeInfo(senderId, id) =>
          val senderIdBytes = senderId.getBytes()
          val idBytes = id.getBytes()
          val buf = ByteBuffer.allocate(4 + 4 + senderIdBytes.length + 4 + idBytes.length)
          buf.putInt(GET_NODE_INFO_TAG)
          putWithSize(buf, senderIdBytes)
          putWithSize(buf, idBytes)
          buf.rewind()
          buf.array()
        case NodeInfo(address, code) =>
          val addressBytes = address.getBytes
          val buf = ByteBuffer.allocate(TAG_SIZE + 4 + CODE_SIZE + addressBytes.length)
            .putInt(NODE_INFO)
            .putInt(addressBytes.length)
            .put(addressBytes)
            .putInt(code)
          buf.rewind()
          buf.array()
        case _ => throw new UnsupportedOperationException()
      }
    }

    override def read(data: Array[Byte]): Event = {
      val buf = ByteBuffer.wrap(data)
      val tag = buf.getInt
      tag match {
        case JOIN_TAG =>
          val nodeId = getString(buf)
          val address = getString(buf)
          val group = getString(buf)
          Join(nodeId = nodeId, address = address, group = group)
        case JOIN_RESULT_TAG =>
          val nodeId = getString(buf)
          val code = buf.getInt
          JoinResult(nodeId, code)
        case RESULT_TAG =>
          val code = buf.getInt
          val msg = getString(buf)
          Result(code, msg)
        case GET_NODE_INFO_TAG => GetNodeInfo(getString(buf), getString(buf))
        case NODE_INFO => NodeInfo(getString(buf), buf.getInt)
        case _ => throw new UnsupportedOperationException()
      }
    }
  }

  private def putWithSize(buf: ByteBuffer, data: Array[Byte]): Unit = {
    buf.putInt(data.length)
    buf.put(data)
  }

  private def getString(buf: ByteBuffer): String = {
    val len = buf.getInt()
    val data = new Array[Byte](len)
    buf.get(data)
    new String(data)
  }

}
