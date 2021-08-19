package io.parapet.core.api

trait Event
object Event {
  case class ByteEvent(data: Array[Byte]) extends Event {
    override def toString: String = new String(data)
  }

  case class StringEvent(value:String) extends Event {
    override def toString: String = value
  }
}
