package io.parapet.spark

import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, AvroSchema, Decoder, Encoder, FromRecord, SchemaFor}
import io.parapet.cluster.node.spark.Spark.Row
import org.apache.avro.{Schema, SchemaBuilder}

import java.io.ByteArrayOutputStream
//
sealed trait Api {
  def toByteArray: Array[Byte] = {
    val os = new ByteArrayOutputStream()
    val aos = AvroOutputStream.binary[Api].to(os).build()
    aos.write(this)
    aos.flush()
    aos.close()
    os.close()
    os.toByteArray
  }
}

object Api {



    sealed trait Task extends Api {
      val id: String
    }

  //implicit val fromRecord = FromRecord[Row]
    case class MapTask(id: String, data: Array[Byte]) extends Task

//  def apply(bytes: Array[Byte]): Api = {
//    val ais = AvroInputStream.binary[Api].from(bytes).build(schema)
//    ais.iterator.toSet.head
//  }
}
