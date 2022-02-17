package io.parapet.spark

import com.typesafe.scalalogging.Logger
import io.parapet.ProcessRef
import io.parapet.core.Process
import io.parapet.spark.Api._

import java.nio.ByteBuffer

class Worker[F[_]](override val ref: ProcessRef, sink: ProcessRef) extends Process[F] {

  private val logger = Logger[Worker[F]]

  override def handle: Receive = {
    case MapTask(clientId, taskId, jobId, data) =>
      logger.debug(s"received mapTask(clientId=$clientId, taskId=$taskId, jobId=$jobId)")
      val buf = ByteBuffer.wrap(data)
      val lambdaSize = buf.getInt()
      val lambdaBytes = new Array[Byte](lambdaSize)
      buf.get(lambdaBytes)
      val f = Codec.deserializeObj(lambdaBytes).asInstanceOf[Row => Row]
      val (schema, rows) = Codec.decodeDataframe(buf)
      val mapped = rows.map(f)
      createMapResult(clientId, taskId, jobId, mapped, schema) ~> sink
  }

  def createMapResult(clientId: ClientId,
                      taskId: TaskId,
                      jobId: JobId,
                      rows: Seq[Row],
                      schema: SparkSchema): MapResult = {
    MapResult(clientId, taskId, jobId, Codec.encodeDataframe(rows, schema))
  }
}