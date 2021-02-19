package io.parapet.cluster.api

import io.parapet.core.{Encoder, Event}

import java.nio.ByteBuffer

object ClusterApi {


  // @formatter:off
  sealed trait API extends Event

  // --------------------- JOIN ------------------------------------------ //
  case class Join(nodeId: String, address: String, group:String) extends API
  // FORMAT
  val TAG_SIZE = 4
  val NODE_ID_SIZE = 4
  val ADDR_SIZE = 4
  val GROUP_SIZE = 4

  // ---------------------- Result ---------------------------------------- //
  case class Result(code: Int, msg: String) extends API
  // result codes

  object ResultCodes {
    val OK = 0
    val ERROR = 1
  }
  // Format
  val CODE_SIZE = 4
  val MSG_SIZE = 4

  // TAGS
  val JOIN_TAG = 0
  val RESULT_TAG = 1

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
        case Result(code, msg) =>
          val msgBytes = msg.getBytes()
          val buf = ByteBuffer.allocate(TAG_SIZE + CODE_SIZE + (MSG_SIZE + msgBytes.length))
          buf.putInt(RESULT_TAG)
          buf.putInt(code)
          putWithSize(buf, msgBytes)
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
        case RESULT_TAG =>
          val code = buf.getInt
          val msg = getString(buf)
          Result(code, msg)
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
