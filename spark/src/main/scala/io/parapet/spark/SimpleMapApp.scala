package io.parapet.spark

import cats.effect.IO
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events
import io.parapet.net.Address
import io.parapet.spark.SparkType._
import io.parapet.{CatsApp, core}

object SimpleMapApp extends CatsApp {

  private val sparkSchema = SparkSchema(Seq(SchemaField("1", IntType)))

  def cluster: DslF[IO, SparkContext[IO]] = {
    SparkContext.builder[IO]
      .clusterMode(true)
      .clusterServers(List(Address.tcp("localhost:7777"), Address.tcp("localhost:7778")))
      .address(Address.tcp("127.0.0.1:4444"))
      .workers(List("worker-1"))
      .build
  }

  def standalone: DslF[IO, SparkContext[IO]] = {
    SparkContext.builder[IO]
      .clusterMode(false)
      .workers(List("worker-1"))
      .workerServers(List(Address.tcp("localhost:5556")))
      .build
  }

  class SimpleMap extends io.parapet.core.Process[IO] {
    override def handle: Receive = {
      case Events.Start =>
        for {
          sparkContext <- standalone
          df <- sparkContext.createDataframe(Seq(Row.of(1)), sparkSchema)
          outDf <- df.map(r => Row.of(r.getAs[Int](0) + 1))
          _ <- outDf.show
        } yield ()
    }
  }

  override def processes(args: Array[String]): IO[Seq[core.Process[IO]]] = IO {
    Seq(new SimpleMap)
  }
}
