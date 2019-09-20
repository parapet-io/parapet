package io.parapet.examples.peer

import cats.effect.IO
import io.parapet.CatsApp
import io.parapet.core.Process
import io.parapet.messaging.ZmqPeer

object Peer2 extends CatsApp {
  override def processes: IO[Seq[Process[IO]]] = {
    for {
      peer <- ZmqPeer[IO]("tcp://*:6666")
     // _ <- peer.run
      con <- peer.connect("tcp://localhost:5555")
      stream <- con.newSteam("text/1.0")
      peerProcess <- IO(new PeerProcess(peer))
      //c <- IO(new Consumer[IO](stream))

    } yield Seq(peerProcess)
  }

}