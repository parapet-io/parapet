package io.parapet.spark

import cats.effect.IO
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events
import io.parapet.{CatsApp, ProcessRef, core}

abstract class DriverApp extends CatsApp {

  val clusterInfo: ClusterInfo

  val sparkContext = new Spark.SparkContext(null)

  // refs
  val nodeRef: ProcessRef = ProcessRef("node")
  val driverRef: ProcessRef = ProcessRef("driver")

  import dsl._

  def createDataframe(schema: SparkSchema, rows: Seq[Row]): DslF[IO, Dataframe] = {
    for {
      df <- eval(new Dataframe(rows, schema, sparkContext))
      _ <- register(driverRef, df)
    } yield df
  }

  def execute: DslF[IO, Unit]

  private class Driver extends io.parapet.core.Process[IO] {
    override def handle: Receive = {
      case Events.Start => execute
    }
  }

  override def processes(args: Array[String]): IO[Seq[core.Process[IO]]] = IO {
    Seq(
      new Driver
    )
  }
}
