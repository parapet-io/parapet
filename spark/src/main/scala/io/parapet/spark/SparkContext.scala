package io.parapet.spark

import cats.effect.Concurrent
import io.parapet.ProcessRef
import io.parapet.cluster.node.NodeProcess
import io.parapet.core.Dsl.DslF
import io.parapet.net.{Address, AsyncClient}
import io.parapet.syntax.FlowSyntax
import org.zeromq.ZContext

class SparkContext[F[_]](override val ref: ProcessRef,
                         workers: List[ProcessRef]) extends io.parapet.core.Process[F] {
  self =>

  import dsl._

  override def handle: Receive = {
    // todo mapResult
    case _ => unit
  }

  def mapDataframe(rows: Seq[Row], schema: SparkSchema, f: Row => Row): DslF[F, Dataframe[F]] = {
    eval(throw new RuntimeException("todo"))
  }

  def createDataframe(rows: Seq[Row], schema: SparkSchema): DslF[F, Dataframe[F]] = {
    eval(new Dataframe[F](rows, schema, self))
  }
}

object SparkContext {

  class Builder[F[_] : Concurrent] extends FlowSyntax[F] {

    import cats.implicits._
    import dsl._

    private val id = "driver-" + System.nanoTime()
    private var _address: Address = _
    private var _clusterMode: Boolean = true
    private var _ioTreads: Int = 1
    private var _clusterServers = List.empty[Address]
    private var _clusterGroup: String = ""
    private var _workers: List[String] = List.empty
    private var _workerServers: List[Address] = List.empty

    def address(value: Address): Builder[F] = {
      _address = value
      this
    }

    def clusterMode(value: Boolean): Builder[F] = {
      _clusterMode = value
      this
    }

    def ioTreads(value: Int): Builder[F] = {
      _ioTreads = value
      this
    }

    def clusterServers(value: List[Address]): Builder[F] = {
      _clusterServers = value
      this
    }

    def clusterGroup(value: String): Builder[F] = {
      _clusterGroup = value
      this
    }

    def workers(value: List[String]): Builder[F] = {
      _workers = value
      this
    }

    def workerServers(value: List[Address]): Builder[F] = {
      _workerServers = value
      this
    }

    def build: DslF[F, SparkContext[F]] = flow {
      val sparkContextRef = ProcessRef(id)
      val nodeRef = ProcessRef(s"node-$id")
      val zmqContext = new ZContext(_ioTreads)

      val workersF =
        if (_clusterMode) {
          for {
            nodeInMapper <- eval(EventMapper[F](sparkContextRef, {
              // todo replace clientId with workerId in MapResult ?
              case NodeProcess.Req(_ /*workerId*/ , data) => Api(data)
            }))
            node <- eval(new NodeProcess[F](nodeRef,
              NodeProcess.Config(id, _address, _clusterServers), nodeInMapper.ref, zmqContext))
            _ <- register(sparkContextRef, nodeInMapper)
            _ <- register(sparkContextRef, node)
            workers <- _workers.map { workerId =>
              val wp = new ClusterWorker[F](workerId, node.ref)
              register(sparkContextRef, wp) ++ eval(wp.ref)
            }.sequence
            // init cluster node
            _ <- NodeProcess.Init ~> node.ref ++ NodeProcess.Join(_clusterGroup) ~> node.ref
          } yield workers

        } else {
          for {
            workers <- _workerServers.zipWithIndex.map { case (address, i) =>
              val name = s"worker-$i"
              val netCli = AsyncClient[F](
                ref = ProcessRef(name),
                zmqContext: ZContext,
                clientId = name,
                address = address)
              register(sparkContextRef, netCli) ++ eval(netCli.ref)
            }.sequence
          } yield (workers)
        }

      for {
        workers <- workersF
        sparkContext <- eval(new SparkContext[F](sparkContextRef, workers))
        _ <- register(ProcessRef.SystemRef, sparkContext)
      } yield sparkContext
    }
  }

  def builder[F[_] : Concurrent]: Builder[F] = new Builder[F]

  class ClusterWorker[F[_]](id: String, nodeRef: ProcessRef) extends io.parapet.core.Process[F] {
    override val ref: ProcessRef = ProcessRef(id)

    override def handle: Receive = {
      case cmd: Api => NodeProcess.Req(id, cmd.toByteArray) ~> nodeRef
    }
  }

}
