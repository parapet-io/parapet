package io.parapet.examples.peer

import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.{InStream, Peer, Process, ProcessRef}
import io.parapet.examples.peer.Api.Ready
import io.parapet.messaging.Utils

import scala.concurrent.duration._

class Consumer[F[_]](peer: Peer[F], target: String, protocolId: String, producer: ProcessRef) extends Process[F] {

  import dsl._

  private var stream: InStream[F] = _

  private val address = Utils.getAddress(peer.info)

  private var ready = false

  private def recv: DslF[F, Unit] = {
    def step: DslF[F, Unit] = flow {

      eval(println("consumer reads from stream")) ++
        suspendWith(stream.read)(bytes => {
          val data = new String(bytes)
          if (data == "READY") {
            if (!ready) {
              eval(ready = true) ++ Ready ~> producer ++ eval(println("Consumer: Ready. sent to " + producer))
            } else unit
          } else eval(println(s"$address received: $data"))

        }) ++ step

    }

    step
  }

  override def handle: Receive = {
    //case Start => suspend(peer.connect(target))
    case Start => suspendWith(peer.connect(target)) { con => {
      require(con != null)
      suspendWith(con.newSteam(protocolId)) { s =>
        suspendWith(s.in) { in =>
          eval(stream = in) //++ delay(2.seconds)
        }
      }
    }
    } ++ blocking(recv)
    case  Stop => suspend(stream.close)
  }
}
