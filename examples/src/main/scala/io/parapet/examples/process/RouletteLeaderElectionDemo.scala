package io.parapet.examples.process

import cats.effect.IO
import io.parapet.core.ProcessRef
import io.parapet.core.processes.RouletteLeaderElection
import io.parapet.core.processes.net.{AsyncClient, AsyncServer}
import io.parapet.{CatsApp, core}

object RouletteLeaderElectionDemo extends CatsApp {

  val p1 = ProcessRef("p1")
  val p2 = ProcessRef("p2")
  val p3 = ProcessRef("p3")

  val peers = Map(
    p1 -> Peer("127.0.0.1", 7775),
    p2 -> Peer("127.0.0.1", 7776),
    p3 -> Peer("127.0.0.1", 7777),
  )

  val netClients: Map[ProcessRef, AsyncClient[IO]] = peers.map {
    case (r, p) => r -> AsyncClient[IO](ProcessRef("net-" + r), p.connect, RouletteLeaderElection.encoder)
  }

  val netServers = peers.map {
    case (p, addr) => AsyncServer[IO](addr.bind, p, RouletteLeaderElection.encoder)
  }

  override def processes: IO[Seq[core.Process[IO]]] = IO {
    val ps = netClients.values ++ netServers ++ peers.keys.map(p => createLeaderElection(p, netClients))
    val seq = ps.toSeq
    seq
  }

  def createLeaderElection(ref: ProcessRef, netClients0: Map[ProcessRef, AsyncClient[IO]]): RouletteLeaderElection[IO] = {
    val peers0 = netClients0.filterKeys(_ != ref).values.map(_.ref).toVector
    new RouletteLeaderElection[IO](new RouletteLeaderElection.State(ref, peers0))
  }

  case class Peer(
                   ip: String,
                   port: Int
                 ) {

    val connect = s"tcp://$ip:$port"
    val bind = s"tcp://*:$port"
  }

}
