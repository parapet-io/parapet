package io.parapet.core

import org.json4s.native.{JsonMethods, Serialization}
import org.json4s.{Formats, FullTypeHints, StringInput}
import io.parapet.core.Encoder._
import io.parapet.core.api.Event

// todo move to a separate module
trait Encoder {
  self =>

  @throws[EncodingException]
  def write(e: Event): Array[Byte]

  @throws[EncodingException]
  def read(data: Array[Byte]): Event
}

object Encoder {

  class JsonEncoder(typeHints: List[Class[_]]) extends Encoder {

    private val typeRegistry: Map[String, Class[_]] =
      typeHints.map(c => c.getName -> c).toMap

    private implicit val formats: Formats =
      Serialization.formats(FullTypeHints(typeHints))

    override def write(e: Event): Array[Byte] =
      try Serialization.write(e).getBytes()
      catch {
        case ex: Throwable => throw EncodingException(s"failed to encode event: $e", ex)
      }

    override def read(data: Array[Byte]): Event = {
      val jsonString = new String(data)
      try {
        val json = JsonMethods.parse(StringInput(jsonString))
        val eventClassName = (json \ "jsonClass").extract[String]
        val eventType = typeRegistry(eventClassName)
        json.extract[Event](formats, Manifest.classType(eventType))
      } catch {
        case ex: Throwable => throw EncodingException(s"failed to parse json: $jsonString", ex)
      }

    }

  }

  def json(typeHints: List[Class[_]]): Encoder = new JsonEncoder(typeHints)

  case class EncodingException(msg: String, cause: Throwable) extends RuntimeException(msg, cause)

}
