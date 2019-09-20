package io.parapet.examples.peer

import cats.effect.IO
import io.parapet.CatsApp
import io.parapet.core.Process
import io.parapet.messaging.ZmqPeer

object Peer1 extends CatsApp {
  override def processes: IO[Seq[Process[IO]]] = {
    for {
      peer <- ZmqPeer[IO]("tcp://*:5555")
      con <- peer.connect("tcp://localhost:6666")
      stream <- con.newSteam("text/1.0")
      peerProcess <- IO(new PeerProcess(peer))
      //p <- IO(new Producer[IO](stream))
      //c <- IO(new Consumer[IO](stream))

    } yield Seq(peerProcess)
  }

}
