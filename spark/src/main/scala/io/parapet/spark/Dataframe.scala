package io.parapet.spark

import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, IO}
import io.parapet.core.Dsl.DslF
import io.parapet.core.api.Cmd.netClient
import io.parapet.spark.Api.{MapResult, MapTask, Task}
import io.parapet.spark.Spark.SparkContext

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class Dataframe(rows: Seq[Row], schema: SparkSchema,
                ctx: SparkContext)(implicit concurrent: Concurrent[IO]) extends io.parapet.core.Process[IO] {

  import dsl._

  private val jobs = new ConcurrentHashMap[String, Job]()
  private val tasks = new ConcurrentHashMap[String, Task]

  def send(tasks: Iterator[Seq[Task]]): DslF[IO, Unit] = {
    def step(idx: Int): DslF[IO, Unit] = {
      if (tasks.hasNext) {
        tasks.next().foldLeft(unit)((acc, task) =>
          acc ++ netClient.Send(task.toByteArray, Option(ref)) ~> ctx.workers(idx)) ++
          step((idx + 1) % ctx.workers.size)
      } else {
        unit
      }
    }

    step(0)
  }

  def map(f: Row => Row): DslF[IO, Dataframe] = {
    val jobId = UUID.randomUUID().toString
    val tasks = rows.map { row =>
      val dfBytes = Codec.encodeDataframe(Seq(row), schema)
      val buf = ByteBuffer.allocate(dfBytes.length + 1000) // random number
      val lambdaBytes = Codec.serializeObj(f)
      buf.putInt(lambdaBytes.length)
      buf.put(lambdaBytes)
      buf.put(dfBytes)
      MapTask(UUID.randomUUID().toString, jobId, Codec.toByteArray(buf))
    }

    println(s"Job[id=$jobId] tasks:")
    tasks.foreach(task => println(s"mapTask id=${task.id}"))

    // split tasks across available workers
    val partitioned = tasks.grouped(tasks.size / ctx.workers.size)

    for {
      done <- suspend(Deferred[IO, Unit])
      job <- eval(new Job(jobId, tasks, done))
      _ <- eval(jobs.put(jobId, job))
      _ <- send(partitioned)
      _ <- suspend(done.get)
      outDf <- eval(new Dataframe(job.results, schema, ctx))
      _ <- eval(println(s"job $jobId completed"))
      _ <- register(ctx.driverRef, outDf)
    } yield outDf // combine results

  }

  def show: DslF[IO, Unit] = dsl.eval {
    rows.foreach { row =>
      println(row.values.mkString(","))
    }
  }

  override def handle: Receive = {
    case netClient.Rep(Some(data)) => Api(data) match {
      case mapResult: MapResult => suspend {
        val taskId = mapResult.id
        val jobId = mapResult.jobId
        println(s"received mapResult(taskId=$taskId, jobId=$jobId)")
        println(s"task id=$taskId completed")
        jobs.get(jobId).complete(mapResult)
      }
    }

  }
}
