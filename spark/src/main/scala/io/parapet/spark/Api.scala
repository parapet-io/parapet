package io.parapet.spark

import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, AvroSchema}
import org.apache.avro.Schema

import java.io.ByteArrayOutputStream

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
  private val schema: Schema = AvroSchema[Api]

  sealed trait Task extends Api {
    val id: String
    val jobId: String
  }

  sealed trait TaskResult extends Api {
    val id: String
    val jobId: String
  }

  case class MapTask(id: String, jobId: String,
                     data: Array[Byte]) extends Task

  case class MapResult(id: String, jobId: String,
                       data: Array[Byte]) extends TaskResult

  def apply(bytes: Array[Byte]): Api = {
    val ais = AvroInputStream.binary[Api].from(bytes).build(schema)
    ais.iterator.toSet.head
  }
}
