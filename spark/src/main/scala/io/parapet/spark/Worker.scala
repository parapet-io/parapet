package io.parapet.spark

import com.typesafe.scalalogging.Logger
import io.parapet.ProcessRef
import io.parapet.core.Process
import io.parapet.spark.Api._

import java.nio.ByteBuffer

class Worker[F[_]](override val ref: ProcessRef, sink: Option[ProcessRef] = Option.empty)
  extends Process[F] {

  private val logger = Logger[Worker[F]]

  import dsl._

  override def handle: Receive = {
    case MapTask(taskId, jobId, data) =>
      withSender { sender =>
        logger.debug(s"received mapTask(taskId=$taskId, jobId=$jobId)")
        val buf = ByteBuffer.wrap(data)
        val lambdaSize = buf.getInt()
        val lambdaBytes = new Array[Byte](lambdaSize)
        buf.get(lambdaBytes)
        val f = Codec.decodeObj(lambdaBytes).asInstanceOf[Row => Row]
        val (schema, rows) = Codec.decodeDataframe(buf)
        val mapped = rows.map(f)
        createMapResult(taskId, jobId, mapped, schema) ~> sink.getOrElse(sender)
      }

  }

  def createMapResult(taskId: TaskId,
                      jobId: JobId,
                      rows: Seq[Row],
                      schema: SparkSchema): MapResult = {
    MapResult(taskId, jobId, Codec.encodeDataframe(rows, schema))
  }
}
