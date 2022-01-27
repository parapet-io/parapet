package io.parapet.spark

import cats.effect.IO
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events
import io.parapet.spark.Spark.Dataframe
import io.parapet.{CatsApp, ProcessRef, core}

abstract class DriverApp extends CatsApp {

  val clusterInfo: ClusterInfo

  val sparkContext = new Spark.SparkContext(null)

  // refs
  val nodeRef = ProcessRef("node")
  val driverRef = ProcessRef("driver")

  def createDataset(schema: SparkSchema, rows: Seq[Row]): Dataframe = {
    new Dataframe(rows, schema, sparkContext)
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
