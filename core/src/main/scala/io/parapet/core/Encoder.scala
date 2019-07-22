package io.parapet.core

import org.json4s.native.{JsonMethods, Serialization}
import org.json4s.{Formats, FullTypeHints, StringInput}

// todo move to a separate module
trait Encoder {
  self =>
  def write(e: Event): Array[Byte]

  def read(data: Array[Byte]): Event
}

object Encoder {

  class JsonEncoder(typeHints: List[Class[_]]) extends Encoder {

    private val typeRegistry: Map[String, Class[_]] =
      typeHints.map(c => c.getName -> c).toMap

    private implicit val formats: Formats =
      Serialization.formats(FullTypeHints(typeHints))

    override def write(e: Event): Array[Byte] = {
      Serialization.write(e).getBytes()
    }

    override def read(data: Array[Byte]): Event = {
      val json = JsonMethods.parse(StringInput(new String(data)))
      val eventClassName = (json \ "jsonClass").extract[String]
      val eventType = typeRegistry(eventClassName)
      json.extract[Event](formats, Manifest.classType(eventType))

    }

  }

  def json(typeHints: List[Class[_]]): Encoder = new JsonEncoder(typeHints)
}