package io.parapet.cluster

import cats.effect.IO
import io.parapet.cluster.Config.PeerInfo
import io.parapet.core.api.Cmd
import io.parapet.core.processes.LeaderElection.{Peer, Peers}
import io.parapet.core.processes.Sub.Subscription
import io.parapet.core.processes.{Coordinator, LeaderElection, Sub}
import io.parapet.core.{EventTransformer, Parapet}
import io.parapet.net.{AsyncClient, AsyncServer}
import io.parapet.{CatsApp, ProcessRef, core}
import org.zeromq.ZContext
import io.parapet.net.Address._
import scopt.OParser

import java.nio.file.Paths

object ClusterApp extends CatsApp {
  // refs
  private val coordinatorRef = ProcessRef("coordinator")
  private val leaderElectionRef = ProcessRef("leader-election")
  private val netServerRef = ProcessRef("net-server")
  private val clusterRef = ProcessRef("cluster")
  private val zmqContext = new ZContext(2)

  private def netClientRef(id: Int): ProcessRef = ProcessRef(s"net-client-$id")

  private val cmdToNetClientSendTransformer = EventTransformer {
    case e: Cmd.coordinator.Api => Cmd.netClient.Send(e.toByteArray)
    case e: Cmd.leaderElection.Api => Cmd.netClient.Send(e.toByteArray)
  }

  override def processes(args: Array[String]): IO[Seq[core.Process[IO]]] = {
    IO.suspend {
      for {
        appArgs <- parseArgs(args)
        config <- loadConfig(appArgs)
        peerNetClients <- createPeerNetClients(config, config.peers)
        peers <- createPeers(config, peerNetClients.toIndexedSeq.map { case (peerInfo, p) => (peerInfo, p.ref) })
        coordinator <- createCoordinator(config, peers)
        sub <- createSub
        netServer <- createServer(sub.ref, config)
        leaderElection <- createLeaderElection(config, clusterRef, peers)
        cluster <- createClusterProcess(config, peers)
        seq <- IO(Seq(coordinator, sub, cluster, leaderElection, netServer) ++ peerNetClients.map(_._2))
      } yield seq
    }
  }

  def loadConfig(appArgs: AppArgs): IO[Config] = IO(Config.load(appArgs.config))

  def createLeaderElection(config: Config,
                           sink: ProcessRef,
                           peers: Peers): IO[LeaderElection[IO]] = IO {
    val state = new LeaderElection.State(
      id = config.id,
      addr = config.address,
      netServer = netServerRef,
      peers = peers,
      coordinatorRef = coordinatorRef)
    new LeaderElection[IO](leaderElectionRef, state, sink)
  }

  def createClusterProcess(config: Config, peers: Peers): IO[ClusterProcess] = {
    IO(new ClusterProcess(clusterRef, config, peers.peers.map(p => p.id -> p.ref).toMap, leaderElectionRef))
  }

  def createServer(sink: ProcessRef, config: Config): IO[AsyncServer[IO]] = IO {
    val port = config.address.split(":")(1).trim.toInt
    AsyncServer[IO](
      ref = netServerRef,
      zmqContext = zmqContext,
      address = tcp("*", port),
      sink = sink)
  }

  def createCoordinator(config: Config,
                        peers: Peers): IO[Coordinator[IO]] = IO {
    new Coordinator[IO](
      coordinatorRef,
      id = config.id,
      client = leaderElectionRef,
      peers = peers.peers.map(peer => peer.id -> peer.ref).toMap,
      threshold = config.coordinatorThreshold,
      timeout = config.coordinatorTimeout)
  }

  def createSub: IO[Sub[IO]] = IO {
    val sub = Sub[IO](Seq(
      Subscription(coordinatorRef, {
        case _: Cmd.coordinator.Api => ()
      }),
      Subscription(leaderElectionRef, {
        case _: Cmd.coordinator.Elected => ()
        case _: Cmd.leaderElection.Api => ()
      })
    ))

    eventTransformer(sub.ref, EventTransformer {
      case e: Cmd.netServer.Message => Cmd(e.data)
    })

    sub
  }

  def createPeerNetClients(config: Config,
                           peers: Array[PeerInfo]): IO[Array[(PeerInfo, AsyncClient[IO])]] = IO {
    val netClients = peers.zipWithIndex
      .map { case (info, index) =>
        info -> AsyncClient[IO](
          ref = netClientRef(index),
          zmqContext = zmqContext,
          clientId = config.id,
          address = tcp(info.address),
          AsyncClient.defaultOpts.withSndHWM(1000))
      }
    netClients.foreach {
      case (_, p) => eventTransformer(p.ref, cmdToNetClientSendTransformer)
    }
    netClients
  }

  def createPeers(config: Config, netClients: Seq[(PeerInfo, ProcessRef)]): IO[Peers] =
    IO {
      Peers(
        netClients.map {
          case (peerInfo, ref) =>
            Peer.builder
              .id(peerInfo.id)
              .address(peerInfo.address)
              .ref(ref)
              .timeoutMs(config.peerTimeout)
              .build
        }.toVector)
    }

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

  override def onExit(): Unit = {
    zmqContext.close()
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
