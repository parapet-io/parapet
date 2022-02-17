package io.parapet.spark

import cats.{Eval => CatsEval}
import io.parapet.ProcessRef
import io.parapet.core.EventLog
import io.parapet.instances.interpreter._
import io.parapet.spark.Api.{ClientId, JobId, MapResult, MapTask, TaskId}
import io.parapet.spark.SparkType.IntType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.ByteBuffer

class WorkerSpec extends AnyFunSuite {

  private val testRef = ProcessRef("test-system")

  private val testSchema = SparkSchema(Seq(SchemaField("intField", IntType)))

  test("submit map task") {
    val sink = new ProcessRef("sink")
    val eventLog = new EventLog()
    val worker = new Worker[CatsEval](ProcessRef("worker"), sink)
    val interpreter = new EvalInterpreter(testRef, eventLog)

    def mapFun(row: Row): Row = Row.of(row.getAs[Int](0) + 1)

    val mapTask = MapTask(ClientId("client-id"),
      TaskId("task-1"), JobId("job-1"),
      encodeMap(mapFun, Seq(Row.of(1)), testSchema))
    worker(mapTask).foldMap(interpreter)


    val mapResult = eventLog.incoming(sink).collect {
      case mr@MapResult(ClientId("client-id"), TaskId("task-1"), JobId("job-1"), _) => mr
    }.head

    val (_, output) = Codec.decodeDataframe(mapResult.data)
    output(0) shouldBe Row.of(2)

  }

  def encodeMap(map: Row => Row, rows: Seq[Row], schema: SparkSchema): Array[Byte] = {
    val buf = ByteBuffer.allocate(1000)
    val mapBytes = Codec.serializeObj(map)
    buf.putInt(mapBytes.length)
    buf.put(mapBytes)
    buf.put(Codec.encodeDataframe(rows, schema))
    Codec.toByteArray(buf)
  }

}
