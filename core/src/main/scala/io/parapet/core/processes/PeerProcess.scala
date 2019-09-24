package io.parapet.core.processes

import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.{Peer, Process}

class PeerProcess[F[_]](peer: Peer[F]) extends Process[F] {

  import dsl._

  override def handle: Receive = {
    case Start => suspend(peer.run)
    case Stop => suspend(peer.stop)
  }
}
