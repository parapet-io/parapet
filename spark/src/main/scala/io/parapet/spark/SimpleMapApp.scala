package io.parapet.spark

import cats.effect.IO
import io.parapet.core.Events
import io.parapet.net.Address
import io.parapet.spark.SparkType._
import io.parapet.{CatsApp, core}

object SimpleMapApp extends CatsApp {

  private val sparkSchema = SparkSchema(Seq(SchemaField("1", IntType)))

  class SimpleMap extends io.parapet.core.Process[IO] {
    override def handle: Receive = {
      case Events.Start =>
        for {
          sparkContext <- SparkContext.builder[IO]
            .clusterMode(false)
            //.workerServers(List(Address.tcp("localhost:5556")))
            .clusterMode(true)
            .clusterServers(List(Address.tcp("localhost:8886"), Address.tcp("localhost:8887")))
            .address(Address.tcp("127.0.0.1:5560"))
            .workers(List("worker-1"))
            .build
          df <- sparkContext.createDataframe(Seq(Row.of(1)), sparkSchema)
          outDf <- df.map(r => r)
          _ <- outDf.show
        } yield ()
    }
  }

  override def processes(args: Array[String]): IO[Seq[core.Process[IO]]] = IO {
    Seq(new SimpleMap)
  }
}
