package io.parapet.examples.peer

import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.Start
import io.parapet.core.{Peer, Process, Stream}
import io.parapet.messaging.Utils

class Consumer[F[_]](peer: Peer[F], target: String, protocolId: String) extends Process[F] {

  import dsl._

  private var stream: Stream[F] = _

  private def rcv: DslF[F, Unit] = {
    def step: DslF[F, Unit] = flow {
      blocking {
        suspendWith(stream.read)(data => {
          eval(println(s"${Utils.getAddress(peer.info)} received: ${new String(data)}"))
        })
      } ++ step
    }

    step
  }

  override def handle: Receive = {
    case Start => suspend(peer.connect(target))
//    case Start => suspendWith(peer.connect(target)) { con => {
//      suspendWith(con.newSteam(protocolId)) { s => eval(stream = s) }
//    }
//    } ++ rcv
  }
}
