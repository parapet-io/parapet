package io.parapet.spark

import cats.effect.IO
import io.parapet.spark.Api.{MapResult, MapTask}
import io.parapet.spark.Api
import io.parapet.{CatsApp, ProcessRef, core}
import io.parapet.core.api.Cmd.netServer

import java.nio.ByteBuffer

class WorkerApp extends CatsApp {

  val serverRef: ProcessRef = ProcessRef("server")

  class Worker extends io.parapet.core.Process[IO] {
    override def handle: Receive = {
      case netServer.Message(clientId, msg) =>
        Api(msg) match {
          case MapTask(taskId, jobId, data) =>
            val buf = ByteBuffer.wrap(data)
            val lambdaSize = buf.getInt()
            val lambdaArr = new Array[Byte](lambdaSize)
            buf.get(lambdaArr)
            val f = Codec.deserializeObj(lambdaArr).asInstanceOf[Row => Row]
            val (schema, rows) = Codec.decodeDataframe(buf)
            val mapped = rows.map(f)
            Codec.encodeDataframe(mapped, schema)
            netServer.Send(clientId, createMapResult(taskId, jobId, mapped, schema)) ~> serverRef
        }
    }

    def createMapResult(taskId: String, jobId: String,
                        rows: Seq[Row], schema: SparkSchema): Array[Byte] = {
      MapResult(taskId, jobId, Codec.encodeDataframe(rows, schema)).toByteArray
    }

  }

  override def processes(args: Array[String]): IO[Seq[core.Process[IO]]] = {
    null // ??
  }
}
