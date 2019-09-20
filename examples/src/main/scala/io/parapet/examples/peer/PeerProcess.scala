package io.parapet.examples.peer

import cats.effect.Concurrent
import io.parapet.core.Event.Start
import io.parapet.core.{Peer, Process}

class PeerProcess[F[_] : Concurrent](peer: Peer[F]) extends Process[F] {

  import dsl._

  override def handle: Receive = {
    case Start => suspend(peer.run)
  }
}
