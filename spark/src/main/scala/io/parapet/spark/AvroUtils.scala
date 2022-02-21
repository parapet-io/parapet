package io.parapet.spark

import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, Decoder, Encoder, SchemaFor}

import java.io.ByteArrayOutputStream
import scala.util.{Failure, Success, Using}

object AvroUtils {
  def encode[T: Encoder](obj: T): Array[Byte] = {
    Using.Manager { use =>
      val bos = use(new ByteArrayOutputStream())
      val os = use(AvroOutputStream.binary[T].to(bos).build())
      os.write(obj)
      os.flush()
      bos.toByteArray
    } match {
      case Failure(err) => throw err
      case Success(value) => value
    }
  }

  def decode[T: Decoder : SchemaFor](bytes: Array[Byte]): T = {
    val ais = AvroInputStream.binary[T].from(bytes).build(implicitly[SchemaFor[T]].schema)
    ais.iterator.toSet.head
  }
}
