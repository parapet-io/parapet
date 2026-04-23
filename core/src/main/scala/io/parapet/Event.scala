package io.parapet

trait Event

object Event:
  final case class ByteEvent(data: Array[Byte]) extends Event:
    override def toString: String = new String(data)

  final case class StringEvent(value: String) extends Event:
    override def toString: String = value
