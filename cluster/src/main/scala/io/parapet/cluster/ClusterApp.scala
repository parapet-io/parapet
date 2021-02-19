package io.parapet.cluster

import cats.effect.{IO, Resource}
import io.parapet.core.processes.RouletteLeaderElection
import io.parapet.core.processes.RouletteLeaderElection.Peers
import io.parapet.core.processes.net.{AsyncClient, AsyncServer}
import io.parapet.core.{Process, ProcessRef}
import io.parapet.{CatsApp, core}

import java.io.FileInputStream
import java.util.Properties
import scala.concurrent.duration._

object ClusterApp extends CatsApp {

  val NodePropsPath = "etc/node.properties"
  //val NodePropsPath = "d:\\dev\\parapet\\cluster\\server-2\\parapet-cluster-0.0.1-RC4\\etc\\node.properties"

  override def processes(args: Array[String]): IO[Seq[core.Process[IO]]] =
    for {
      config <- loadConfig
      peerNetClients <- createPeerNetClients(config.id, config.peers)
      peers <- IO(Peers(peerNetClients.mapValues(_.ref), config.peerTimeout.toMillis.longValue()))
      leRef <- IO.pure(ProcessRef(config.id))
      srv <- IO {
        val port = config.address.split(":")(1).trim.toInt
        AsyncServer[IO](
          ref = ProcessRef("net-server"),
          address = s"tcp://*:$port",
          sink = leRef,
          encoder = RouletteLeaderElection.encoder,
        )
      }
      leState <- IO.pure(
        new RouletteLeaderElection.State(
          ref = leRef,
          addr = config.address,
          srvRef = srv.ref,
          peers = peers,
          threshold = config.leaderElectionThreshold,
        ),
      )
      cluster <- IO(new ClusterProcess(leState.ref))
      le <- IO(new RouletteLeaderElection[IO](leState, cluster.ref))
      seq <- IO {
        Seq(cluster, le, srv) ++ peerNetClients.values
      }
    } yield seq

  def loadConfig: IO[Config] =
    Resource.fromAutoCloseable(IO(new FileInputStream(NodePropsPath))).use { input =>
      IO {
        val prop = new Properties()
        prop.load(input)
        logger.info(s"Config: $prop")
        Config(
          id = prop.getProperty("node.id"),
          address = prop.getProperty("node.address"),
          peers = prop.getProperty("node.peers", "").split(",").map(_.trim),
          electionDelay = prop.getProperty("node.election-delay").toInt.seconds,
          heartbeatDelay = prop.getProperty("node.heartbeat-delay").toInt.seconds,
          monitorDelay = prop.getProperty("node.monitor-delay").toInt.seconds,
          peerTimeout = prop.getProperty("node.peer-timeout").toInt.seconds,
          leaderElectionThreshold = prop.getProperty("node.leader-election-threshold").toDouble,
        )
      }
    }

  def createPeerNetClients(clientId: String, addresses: Array[String]): IO[Map[String, Process[IO]]] =
    IO(
      addresses.zipWithIndex
        .map(p =>
          p._1 -> AsyncClient[IO](
            ref = ProcessRef("peer-client-" + p._2),
            clientId = clientId,
            address = "tcp://" + p._1,
            encoder = RouletteLeaderElection.encoder,
          ),
        )
        .toMap,
    )

}
