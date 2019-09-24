package io.parapet.examples.peer

import cats.effect.IO
import io.parapet.CatsApp
import io.parapet.core.Peer.PeerInfo
import io.parapet.core.{Peer, Process}
import io.parapet.messaging.ZmqPeer

abstract class AbstractPeer extends CatsApp {

  val peerInfo: PeerInfo
  def producer(peer:Peer[IO]) : Process[IO]
  def consumer(peer:Peer[IO]) : Process[IO]

  override def processes: IO[Seq[Process[IO]]] = {
    for {
      peer <- ZmqPeer[IO](peerInfo)
      consumer <- IO(consumer(peer))
      producer <- IO(producer(peer))

    } yield Seq(peer.process, consumer, producer)
  }

}