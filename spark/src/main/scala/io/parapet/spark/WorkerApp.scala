package io.parapet.spark

import cats.effect.IO
import com.typesafe.scalalogging.Logger
import io.parapet.core.api.Cmd.netServer
import io.parapet.net.{Address, AsyncServer}
import io.parapet.spark.Api.{ClientId, JobId, MapResult, MapTask, TaskId}
import io.parapet.{CatsApp, ProcessRef, core}
import org.slf4j.LoggerFactory
import org.zeromq.ZContext

import java.nio.ByteBuffer

abstract class WorkerApp extends CatsApp {

  val address: String

  val workerRef: ProcessRef = ProcessRef("worker")

  val serverRef: ProcessRef = ProcessRef("server")

  class Worker(zmqCtx: ZContext) extends io.parapet.core.Process[IO] {
    override val ref: ProcessRef = workerRef

    override def handle: Receive = {
      case netServer.Message(clientId, msg) =>
        Api(msg) match {
          case MapTask(clientId, taskId, jobId, data) =>
            logger.debug(s"received mapTask(taskId=$taskId, jobId=$jobId)")
            val buf = ByteBuffer.wrap(data)
            val lambdaSize = buf.getInt()
            val lambdaArr = new Array[Byte](lambdaSize)
            buf.get(lambdaArr)
            val f = Codec.deserializeObj(lambdaArr).asInstanceOf[Row => Row]
            val (schema, rows) = Codec.decodeDataframe(buf)
            val mapped = rows.map(f)
            Codec.encodeDataframe(mapped, schema)
            netServer.Send(clientId.underlying, createMapResult(clientId, taskId, jobId, mapped, schema)) ~> serverRef
        }
    }

    def createMapResult(
                         clientId: ClientId,
                         taskId: TaskId, jobId: JobId,
                        rows: Seq[Row], schema: SparkSchema): Array[Byte] = {
      MapResult(clientId, taskId, jobId, Codec.encodeDataframe(rows, schema)).toByteArray
    }

    private def show(rows: Array[Row]): Unit = {
      rows.foreach { row =>
        logger.debug(row.values.mkString(","))
      }
    }

  }

  override def processes(args: Array[String]): IO[Seq[core.Process[IO]]] = IO {
    val zmqContext = new ZContext(1)
    val worker = new Worker(zmqContext)
    Seq(
      worker,
      AsyncServer(serverRef, zmqContext, Address(address), worker.ref)
    )
  }
}
