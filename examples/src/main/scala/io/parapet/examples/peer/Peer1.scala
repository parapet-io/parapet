package io.parapet.examples.peer

import cats.effect.IO
import io.parapet.CatsApp
import io.parapet.core.{Peer, Ports, Process, Stream}
import io.parapet.core.Event.Start
import io.parapet.core.Peer.PeerInfo
import io.parapet.messaging.ZmqPeer

object Peer1 extends AbstractPeer {

  override val peerInfo: PeerInfo = PeerInfo(
    protocol = "tcp",
    host = "localhost",
    port = 5555,
    protocols = Set("text/1.0"),
    ports = Ports(5556, 5656)
  )

  override def producer(peer: Peer[IO]): Process[IO] =
    new Producer(peer, "tcp://localhost:6666", "text/1.0", Seq("peer1-1", "peer1-2"))

  override def consumer(peer: Peer[IO]): Process[IO] = Process[IO](_ => {
    case _ => dsl.unit
  })
}
