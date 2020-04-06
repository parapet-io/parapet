package io.parapet.core.doc


object Doc {

  class Doc(value: String) extends scala.annotation.StaticAnnotation

  class Spec(description: String, api: Api) extends scala.annotation.StaticAnnotation

  case class Api(in: Array[Class[_ <: io.parapet.core.Event]] = Array.empty, out: Array[Class[_ <: io.parapet.core.Event]])


  object Meta {

    case class Spec(description: String, api: Api)

    case class Api(in: Array[Event], out: Array[Event])

    case class Event(description: String, params: Seq[EventParam])

    case class EventParam(name: String, description: String)

    def create(spec: Doc.Spec): Meta.Spec = {
      null
    }

    trait MetaWriter {
      def write(spec: Meta.Spec): Array[Byte]
    }

    object JsonWriter extends MetaWriter {
      override def write(spec: Meta.Spec): Array[Byte] = {
        Array.empty[Byte]
      }
    }

  }


}
