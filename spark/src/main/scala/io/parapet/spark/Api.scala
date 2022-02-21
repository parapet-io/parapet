package io.parapet.spark

import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, AvroSchema}
import io.parapet.Event
import org.apache.avro.Schema

import java.io.ByteArrayOutputStream

sealed trait Api extends Event {
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

  // Opaque Type Aliases
  case class ClientId(val underlying: String) extends AnyVal
  case class TaskId(val underlying: String) extends AnyVal
  case class JobId(val underlying: String) extends AnyVal

  sealed trait Task extends Api {
   // val clientId:ClientId
    val taskId: TaskId
    val jobId: JobId
  }

  sealed trait TaskResult extends Api {
    //val clientId:ClientId
    val taskId: TaskId
    val jobId: JobId
  }

  case class MapTask(taskId: TaskId,
                     jobId: JobId,
                     data: Array[Byte]) extends Task

  case class MapResult(taskId: TaskId,
                       jobId: JobId,
                       data: Array[Byte]) extends TaskResult

  def apply(bytes: Array[Byte]): Api = {
    val ais = AvroInputStream.binary[Api].from(bytes).build(schema)
    ais.iterator.toSet.head
  }
}
