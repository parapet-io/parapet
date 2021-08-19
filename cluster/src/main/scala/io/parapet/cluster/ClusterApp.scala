package io.parapet.cluster

import cats.effect.IO
import io.parapet.cluster.Config.PeerInfo
import io.parapet.core.processes.RouletteLeaderElection
import io.parapet.core.processes.RouletteLeaderElection.Peers
import io.parapet.core.processes.net.{AsyncClient, AsyncServer}
import io.parapet.core.{Parapet, Process, ProcessRef}
import io.parapet.{CatsApp, core}
import scopt.OParser

import java.nio.file.Paths

object ClusterApp extends CatsApp {

  override def processes(args: Array[String]): IO[Seq[core.Process[IO]]] =
    for {
      appArgs <- parseArgs(args)
      config <- loadConfig(appArgs)
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
          sink = leRef)
      }
      leState <- IO.pure(
        new RouletteLeaderElection.State(
          ref = leRef,
          id = config.id,
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

  def loadConfig(appArgs: AppArgs): IO[Config] = IO(Config.load(appArgs.config))

  def createPeerNetClients(clientId: String, peers: Array[PeerInfo]): IO[Array[(PeerInfo, Process[IO])]] =
    IO(
      peers.zipWithIndex
        .map(p =>
          p._1 -> AsyncClient[IO](
            ref = ProcessRef("peer-client-" + p._2),
            clientId = clientId,
            address = "tcp://" + p._1.address),
        )
    )

  private val builder = OParser.builder[AppArgs]
  private val parser = {
    import builder._
    OParser.sequence(
      programName("parapet-cluster"),
      head("cluster", Parapet.Version),
      opt[String]('c', "config")
        .action((x, c) => c.copy(config = x))
        .text("path to config file"),
    )
  }

  case class AppArgs(config: String = "etc/node.properties")

  private def parseArgs(args: Array[String]): IO[AppArgs] = {
    IO {
      println(System.getProperty("user.dir"))
      println(Paths.get("").toAbsolutePath.toString)
      OParser.parse(parser, args, AppArgs()) match {
        case Some(appArgs) => appArgs
        case _ => throw new IllegalArgumentException("bad program args")
      }
    }
  }
}
