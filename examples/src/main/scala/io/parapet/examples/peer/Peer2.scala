package io.parapet.examples.peer

import cats.effect.IO
import io.parapet.CatsApp
import io.parapet.core.Event.Start
import io.parapet.core.Peer.PeerInfo
import io.parapet.core.{Peer, Ports, Process, ProcessRef}
import io.parapet.messaging.ZmqPeer

object Peer2 extends AbstractPeer {

  override val peerInfo: PeerInfo = PeerInfo(
    protocol = "tcp",
    host = "localhost",
    port = 6666,
    protocols = Set("text/1.0"),
    ports = Ports(6667, 6766)
  )

  override def producer(peer: Peer[IO]): Process[IO] =
    new Producer(peer, "tcp://localhost:5555", "text/1.0", Seq("msg-3", "msg-4"))

  override def consumer(peer: Peer[IO], producerRef: ProcessRef): Process[IO] =
    new Consumer[IO](peer, "tcp://localhost:5555", "text/1.0", producerRef)
}