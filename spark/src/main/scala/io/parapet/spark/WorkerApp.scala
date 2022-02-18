package io.parapet.spark

import cats.effect.IO
import io.parapet.cluster.node.NodeProcess
import io.parapet.net.{Address, AsyncServer}
import io.parapet.{CatsApp, ProcessRef, core}
import org.zeromq.ZContext

import java.io.FileInputStream
import java.util.Properties
import scala.util.Using


object WorkerApp extends CatsApp {

  val workerRef: ProcessRef = ProcessRef("worker")
  val serverRef: ProcessRef = ProcessRef("server")
  val nodeRef: ProcessRef = ProcessRef("node")

  override def processes(args: Array[String]): IO[Seq[core.Process[IO]]] = IO {
    val config = parseConfig(args)
    val props = loadProperties(config.configPath)
    val workerId = props.id
    val address = props.address
    val clusterServers = props.servers
    val clusterMode = clusterServers.isEmpty
    val workerRef = ProcessRef(workerId)
    val nodeRef = ProcessRef(s"node-$workerId")
    val serverRef = ProcessRef(s"server-$workerId")

    val zmqContext = new ZContext(1)

    val router = new Router[IO](clusterMode, workerRef, if (clusterMode) nodeRef else serverRef)

    val backend = if (clusterMode) {
      AsyncServer[IO](serverRef, zmqContext, address, router.ref)
    } else {
      new NodeProcess[IO](nodeRef, NodeProcess.Config(id, address, clusterServers), router.ref, zmqContext)
    }

    Seq(new Worker[IO](workerRef, router.ref), backend)
  }


  case class Config(configPath: String = "")

  import scopt.OParser

  private val builder = OParser.builder[Config]
  private val parser = {
    import builder._
    OParser.sequence(
      programName("worker"),
      head("worker", "1.0"),

      opt[String]('c', "config")
        .required()
        .action((x, c) => c.copy(configPath = x))
        .text("path to properties file")
    )
  }

  def parseConfig(args: Array[String]): Config = {
    OParser.parse(parser, args, Config()) match {
      case Some(config) => config
      case _ => throw new RuntimeException("bad args")
    }
  }

  def loadProperties(path: String): Properties = {
    Using.resource(new FileInputStream(path)) { is =>
      val prop = new Properties()
      prop.load(is)
      prop
    }
  }

  implicit class PropertiesOps(props: Properties) {

    def id: String = {
      Option(props.getProperty(WorkerApp.address)).map(_.trim).filter(_.nonEmpty) match {
        case Some(value) => value
        case None => throw new RuntimeException("worker id is required")
      }
    }

    def address: Address = {
      Option(props.getProperty(WorkerApp.address)).map(v => Address(v.trim)) match {
        case Some(value) => value
        case None => throw new RuntimeException("worker server is required")
      }
    }

    def servers: Array[Address] = {
      Option(props.getProperty(WorkerApp.servers))
        .map(_.split(",").map(_.trim).map(Address.tcp)).getOrElse(Array.empty)
    }
  }

  /**
    * ===========================
    * Properties
    * ===========================
    */
  val id = "id"
  val address = "address"
  val servers = "worker.cluster-servers"

}