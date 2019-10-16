package io.parapet.examples.peer

import cats.effect.IO
import io.parapet.CatsApp
import io.parapet.core.Peer.PeerInfo
import io.parapet.core.{Peer, Process, ProcessRef}
import io.parapet.messaging.ZmqPeer

abstract class AbstractPeer extends CatsApp {

  val peerInfo: PeerInfo

  def producer(peer: Peer[IO]): Process[IO]

  def consumer(peer: Peer[IO], producer: ProcessRef): Process[IO]

  override def processes: IO[Seq[Process[IO]]] = {
    for {
      peer <- ZmqPeer[IO](peerInfo)
      producer <- IO(producer(peer))
      consumer <- IO(consumer(peer, producer.ref))
    } yield Seq(peer.process, consumer, producer)
  }

}