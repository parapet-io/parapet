package io.parapet.cluster.api

import io.parapet.core.{Encoder, Event}

import java.nio.ByteBuffer

object ClusterApi {

  // @formatter:off
  sealed trait API extends Event
  case class Join(group:String, addr: String) extends API
  // FORMAT
  val TAG_SIZE = 4
  val GROUP_SIZE = 4
  val ADDR_SIZE = 4
  // TAGS
  val JOIN = 0
  // @formatter:on

  val encoder: Encoder = new Encoder {
    override def write(e: Event): Array[Byte] = {
      e match {
        // we don't need to send id because it's set as ZMQ socket identity
        case Join(group, addr) => {
          val groupBytes = group.getBytes()
          val addrBytes = addr.getBytes()
          val buf = ByteBuffer.allocate(TAG_SIZE + GROUP_SIZE + groupBytes.length + ADDR_SIZE + addrBytes.length)
          // write
          buf.putInt(JOIN)
          // group
          buf.putInt(groupBytes.length)
          buf.put(groupBytes)
          // addr
          buf.putInt(addrBytes.length)
          buf.put(addrBytes)
          buf.rewind()
          buf.array()
        }
        case _ => throw new UnsupportedOperationException()
      }
    }

    override def read(data: Array[Byte]): Event = {
      val buf = ByteBuffer.wrap(data)
      val tag = buf.getInt
      tag match {
        case JOIN =>
          val group = getString(buf)
          val addr = getString(buf)
          Join(group, addr)
        case _ => throw new UnsupportedOperationException()
      }
    }
  }

  private def getString(buf: ByteBuffer): String = {
    val len = buf.getInt()
    val data = new Array[Byte](len)
    buf.get(data)
    new String(data)
  }

}
