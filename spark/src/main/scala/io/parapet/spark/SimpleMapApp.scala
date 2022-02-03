package io.parapet.spark

import cats.effect.IO
import io.parapet.core.Dsl.DslF
import io.parapet.spark.SparkType._

object SimpleMapApp extends DriverApp {

  val sparkSchema = SparkSchema(Seq(SchemaField("1", IntType)))

  override val clusterInfo: ClusterInfo =
    ClusterInfo(
      "",
      List.empty,
      List.empty
    )

  import dsl._

  override def execute: DslF[IO, Unit] = flow {
    for {
      df <- createDataframe(sparkSchema, Seq(Row.of(1), Row.of(2)))
      _ <- eval(println("hi"))
      updated <- df.map { r =>
        Row(r.values.map(v => v.asInstanceOf[Int] + 1))
      }
      _ <- eval(println("result df:"))
      _ <- updated.show
    } yield ()
  }

  override val workersAddr: Vector[String] = Vector("tcp://localhost:5555", "tcp://localhost:5556")
}
