package io.parapet.spark

import cats.effect.{Concurrent, IO}
import io.parapet.cluster.node.NodeProcess
import io.parapet.core.Dsl.DslF
import io.parapet.core.api.Cmd.netServer
import io.parapet.core.{Channel, Events}
import io.parapet.net.{Address, AsyncServer}
import io.parapet.{CatsApp, ProcessRef, core}
import org.zeromq.ZContext

import java.io.FileInputStream
import java.util.Properties
import scala.util.{Failure, Success, Using}


object WorkerApp extends CatsApp {

  override def processes(args: Array[String]): IO[Seq[core.Process[IO]]] = IO {
    val config = parseConfig(args)
    val props = loadProperties(config.configPath)
    val workerId = props.id
    val clusterServers = props.servers
    val clusterMode = clusterServers.nonEmpty
    val workerRef = ProcessRef(workerId)
    val backend = if (clusterMode) {
      new ClusterWorker[IO](props, workerRef)
    } else {
      new StandaloneWorker[IO](props, workerRef)
    }

    Seq(new Worker[IO](workerRef), backend)
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
      Option(props.getProperty(WorkerApp.id)).map(_.trim).filter(_.nonEmpty) match {
        case Some(value) => value
        case None => throw new RuntimeException("worker id is required")
      }
    }

    def address: Address = {
      Option(props.getProperty(WorkerApp.address)).map(v => Address.tcp(v.trim)) match {
        case Some(value) => value
        case None => throw new RuntimeException("worker server is required")
      }
    }

    def servers: Array[Address] = {
      Option(props.getProperty(WorkerApp.servers))
        .map(_.split(",").map(_.trim).map(Address.tcp)).getOrElse(Array.empty)
    }

    def clusterGroup: String = {
      Option(props.getProperty(WorkerApp.clusterGroup)).map(_.trim).getOrElse("")
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
  val clusterGroup = "worker.cluster-group"

  // receives messages from AsyncServer process and forwards to Worker
  // implements strict request - reply dialog
  class StandaloneWorker[F[_] : Concurrent](props: Properties, backend: ProcessRef) extends io.parapet.core.Process[F] {

    import dsl._

    private lazy val zmqContext: ZContext = new ZContext(1)
    private lazy val server = AsyncServer[F](ProcessRef(s"${props.id}-server"), zmqContext, props.address, ref)

    private val chan = Channel[F]

    override def handle: Receive = {
      case Events.Start => register(ref, chan) ++ register(ref, server)
      case netServer.Message(clientId, data) =>
        chan.send(Api(data), backend).flatMap {
          case Failure(exception) => raiseError(exception) // uh oh
          case Success(value: Api) => netServer.Send(clientId, value.toByteArray) ~> server.ref
        }
    }
  }

  class ClusterWorker[F[_] : Concurrent](props: Properties, backend: ProcessRef)
    extends io.parapet.core.Process[F] {

    import dsl._

    private lazy val zmqContext: ZContext = new ZContext(1)
    private lazy val node = new NodeProcess[F](ProcessRef(s"${props.id}-node"),
      NodeProcess.Config(props.id, props.address, props.servers), ref, zmqContext)
    private val chan = new Channel[F](ProcessRef("worker-chan"))

    private def init: DslF[F, Unit] = {
      register(ref, node) ++ register(ref, chan) ++
        NodeProcess.Init ~> node.ref ++ NodeProcess.Join(props.clusterGroup) ~> node.ref
    }

    override def handle: Receive = {
      case Events.Start => init
      case NodeProcess.Req(clientId, data) =>
        chan.send(Api(data), backend).flatMap {
          case Failure(exception) => raiseError(exception) // uh oh
          case Success(event: Api) => NodeProcess.Req(clientId, event.toByteArray) ~> node.ref
          case Success(event) => eval(logger.warn(s"unknown event: $event"))
        }
    }
  }

}