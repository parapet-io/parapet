package io.parapet.spark

import cats.effect.Concurrent
import cats.effect.concurrent.Deferred
import com.typesafe.scalalogging.Logger
import io.parapet.ProcessRef
import io.parapet.cluster.node.NodeProcess
import io.parapet.core.Dsl.DslF
import io.parapet.net.{Address, AsyncClient}
import io.parapet.spark.Api.{JobId, MapResult, MapTask, TaskId}
import io.parapet.syntax.FlowSyntax
import org.zeromq.ZContext

import java.nio.ByteBuffer
import java.util.UUID
import scala.collection.mutable

class SparkContext[F[_] : Concurrent](override val ref: ProcessRef,
                                      workers: List[ProcessRef]) extends io.parapet.core.Process[F] {
  self =>

  import dsl._

  private val logger = Logger[SparkContext[F]]
  private val jobs = mutable.Map.empty[JobId, Job[F]]

  override def handle: Receive = {
    case res@MapResult(taskId, jobId, _) =>
      eval(logger.debug(s"received mapResult[jobId=$jobId, taskId=$taskId]")) ++
        jobs(res.jobId).complete(res)
  }

  def mapDataframe(rows: Seq[Row], schema: SparkSchema, f: Row => Row): DslF[F, Dataframe[F]] = flow {
    val lambdaBytes = Codec.encodeObj(f)
    val jobId = JobId(UUID.randomUUID().toString)
    val mapTasks = rows.grouped(Math.max(1, rows.size / workers.size)).map { batch =>
      val taskId = TaskId(UUID.randomUUID().toString)
      val dfBytes = Codec.encodeDataframe(batch, schema)
      val buf = ByteBuffer.allocate(4 + lambdaBytes.length + dfBytes.length)
      buf.putInt(lambdaBytes.length)
      buf.put(lambdaBytes)
      buf.put(dfBytes)
      MapTask(taskId, jobId, Codec.toByteArray(buf))
    }.toList

    // round robin
    var idx = 0

    def nextWorker: ProcessRef = {
      val tmp = workers(idx)
      idx = idx + 1
      idx = idx % workers.size
      tmp
    }

    for {
      signal <- suspend(Deferred[F, List[Row]])
      _ <- eval {
        jobs += jobId -> new Job[F](mapTasks.map(_.taskId).toSet, signal)
      }
      _ <- par(mapTasks.map(t => t ~> nextWorker).toSeq: _*)
      result <- suspend(signal.get)
      dataframe <- createDataframe(result, schema)
    } yield dataframe
  }

  def createDataframe(rows: Seq[Row], schema: SparkSchema): DslF[F, Dataframe[F]] = {
    eval(new Dataframe[F](rows, schema, self))
  }
}

object SparkContext {

  class Builder[F[_] : Concurrent] extends FlowSyntax[F] {

    import cats.implicits._
    import dsl._

    private var _id = "driver-" + System.nanoTime()
    private var _address: Address = _
    private var _clusterMode: Boolean = true
    private var _ioTreads: Int = 1
    private var _clusterServers = List.empty[Address]
    private var _clusterGroup: String = ""
    private var _workers: List[String] = List.empty
    private var _workerServers: List[Address] = List.empty

    def id(value: String): Builder[F] = {
      _id = value
      this
    }

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
      val sparkContextRef = ProcessRef(_id)
      val nodeRef = ProcessRef("driver") // ProcessRef(s"node-$id")
      val zmqContext = new ZContext(_ioTreads)

      val workersF =
        if (_clusterMode) {
          for {
            nodeInMapper <- eval(EventMapper[F](sparkContextRef, {
              // todo replace clientId with workerId in MapResult ?
              case NodeProcess.Req(_ /*workerId*/ , data) => Api(data)
            }))
            node <- eval(new NodeProcess[F](nodeRef,
              NodeProcess.Config(_id, _address, _clusterServers), nodeInMapper.ref, zmqContext))
            _ <- register(ProcessRef.SystemRef, nodeInMapper)
            _ <- register(ProcessRef.SystemRef, node)
            workers <- _workers.map { workerId =>
              val wp = new ClusterWorker[F](workerId, node.ref)
              register(ProcessRef.SystemRef, wp) ++ eval(wp.ref)
            }.sequence
            // init cluster node
            _ <- NodeProcess.Init ~> node.ref ++ NodeProcess.Join(_clusterGroup) ~> node.ref
          } yield workers

        } else {
          for {
            workers <- _workerServers.zipWithIndex.map { case (address, i) =>
              val name = s"worker-$i"
              val netClient = AsyncClient[F](
                ref = ProcessRef(name),
                zmqContext: ZContext,
                clientId = name,
                address = address)
              val worker = new StandaloneWorker[F](netClient.ref, sparkContextRef)
              register(ProcessRef.SystemRef, netClient) ++
                register(ProcessRef.SystemRef, worker) ++
                eval(worker.ref)
            }.sequence
          } yield workers
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

  class StandaloneWorker[F[_]](netClientRef: ProcessRef, sinkRef: ProcessRef) extends io.parapet.core.Process[F] {

    import dsl._
    import io.parapet.core.api.Cmd.netClient

    override def handle: Receive = {
      case cmd: Api =>
        netClient.Send(cmd.toByteArray, Option(ref)) ~> netClientRef
      case netClient.Rep(Some(data)) => Api(data) ~> sinkRef
      case _ => unit
    }
  }

}
