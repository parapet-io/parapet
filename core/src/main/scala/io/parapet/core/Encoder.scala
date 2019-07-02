package io.parapet.core

import org.json4s.{DefaultFormats, Extraction, JValue}

// todo move to a separate module
trait Encoder {
  self =>
  def write(e: Event): Array[Byte]

  def read(data: Array[Byte]): Event
}

object Encoder {

  class JsonEncoder(typeHints: List[Class[_]]) extends Encoder {

    import org.json4s.JsonDSL._
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    import org.json4s.native.Serialization


    private val typeRegistry: Map[String, Class[_]] = typeHints.map(c => c.getCanonicalName -> c).toMap
    private implicit val formats: Formats = Serialization.formats(FullTypeHints(typeHints))

    override def write(e: Event): Array[Byte] = {
      val json = ("eventType" -> e.getClass.getCanonicalName) ~ ("event" -> toJValue(e))
      val jsonString = compact(render(json))
      jsonString.getBytes()
    }

    override def read(data: Array[Byte]): Event = {

      val jsonString = new String(data)
      val json = parse(StringInput(jsonString))
      val eventClassName = (json \ "eventType").extract[String]
      val eventType = typeRegistry(eventClassName)
      val event: Event = (json \ "event").extract[Event](formats, Manifest.classType(eventType))

      event.asInstanceOf[Event]
    }
  }

  def json(typeHints: List[Class[_]]): Encoder = new JsonEncoder(typeHints)


  private def toJValue(obj: AnyRef): JValue = {
    Extraction.decompose(obj)(DefaultFormats)
  }

  case class TestEvent(data: String) extends Event

}