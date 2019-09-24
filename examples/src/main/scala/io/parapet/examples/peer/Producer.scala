package io.parapet.examples.peer

import io.parapet.core.Event.Start
import io.parapet.core.{Peer, Process, Stream}

class Producer[F[_]](peer: Peer[F],
                     target: String,
                     protocolId: String, msgSeq: Seq[String]) extends Process[F] {

  import dsl._

  private var stream: Stream[F] = _

  override def handle: Receive = {
    case Start => suspendWith(peer.connect(target)) { con => {
      suspendWith(con.newSteam(protocolId)) { s => eval(stream = s) }
    }
    } ++ blocking {
      msgSeq.map(msg => suspend(stream.write(msg.getBytes))).fold(unit)(_ ++ _)
    }
  }
}