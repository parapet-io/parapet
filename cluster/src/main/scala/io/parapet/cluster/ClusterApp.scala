package io.parapet.cluster

import cats.effect.{IO, Resource}
import io.parapet.cluster.Config.PeerInfo
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

  override def processes(args: Array[String]): IO[Seq[core.Process[IO]]] =
    for {
      config <- loadConfig
      peerNetClients <- createPeerNetClients(config.id, config.peers)
      peers <- IO(Peers(peerNetClients.map{
        case (peerInfo, netClient) =>
          Peers.builder
            .id(peerInfo.id)
            .address(peerInfo.address)
            .netClient(netClient.ref)
            .timeoutMs(config.peerTimeout)
             .build
      }.toVector))
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
          netServer = srv.ref,
          peers = peers,
          threshold = config.leaderElectionThreshold,
        ),
      )
      cluster <- IO(new ClusterProcess(leState.ref))
      le <- IO(new RouletteLeaderElection[IO](leState, cluster.ref))
      seq <- IO(Seq(cluster, le, srv) ++ peerNetClients.map(_._2))
    } yield seq

  def loadConfig: IO[Config] = IO(Config.load(NodePropsPath))

  def createPeerNetClients(clientId: String, peers: Array[PeerInfo]): IO[Array[(PeerInfo, Process[IO])]] =
    IO(
      peers.zipWithIndex
        .map(p =>
          p._1 -> AsyncClient[IO](
            ref = ProcessRef("peer-client-" + p._2),
            clientId = clientId,
            address = "tcp://" + p._1.address,
            encoder = RouletteLeaderElection.encoder,
          ),
        )
    )

}
