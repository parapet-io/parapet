package io.parapet.spark

import cats.effect.IO
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events
import io.parapet.net.{Address, AsyncClient}
import io.parapet.{CatsApp, ProcessRef, core}
import org.zeromq.ZContext

abstract class DriverApp extends CatsApp {

  val clusterInfo: ClusterInfo

  val workersAddr: Vector[String]

  private var sparkContext: Spark.SparkContext = _ // new Spark.SparkContext(workers)

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
    override val ref: ProcessRef = ProcessRef("driver")

    override def handle: Receive = {
      case Events.Start => execute
    }
  }

  override def processes(args: Array[String]): IO[Seq[core.Process[IO]]] = IO {
    val zmqContext = new ZContext(1)
    val workers = workersAddr.zipWithIndex.map {
      case (address, idx) =>
        val name = s"worker-$idx"
        AsyncClient[IO](
          ref = ProcessRef(name),
          zmqContext: ZContext,
          clientId = name,
          address = Address(address))
    }
    sparkContext = new Spark.SparkContext(workers.map(_.ref), zmqContext)
    workers :+ new Driver
  }
}
