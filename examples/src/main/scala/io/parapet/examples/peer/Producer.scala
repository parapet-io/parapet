package io.parapet.examples.peer

import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.Start
import io.parapet.core.{OutStream, Peer, Process}
import io.parapet.examples.peer.Api.Ready

import scala.concurrent.duration._

class Producer[F[_]](peer: Peer[F],
                     target: String,
                     protocolId: String, msgSeq: Seq[String]) extends Process[F] {

  import dsl._

  private var stream: OutStream[F] = _

  private var ready = false

  override def handle: Receive = {
    case Ready => eval {
      println("Producer::Ready")
      ready = true
    } ++ sendMessages

    case Start => suspendWith(peer.connect(target)) { conn => {
      require(conn != null)
      suspendWith(conn.newSteam(protocolId)) { s =>
        require(s != null)
        suspendWith(s.out) { out =>
          eval(println(s"producer created stream")) ++ eval(stream = out) //++ delay(2.seconds)
        }
      }
    }
    } ++ eval(println("Producer ref=" + ref)) ++ fork(sendReady)  ++ delay(3.second) //++ Ready ~> ref
  }

  def sendMessages: DslF[F, Unit] =
    msgSeq.map(msg => suspend(stream.write(msg.getBytes)) ++ eval(println(s"producer sent: $msg"))).fold(unit)(_ ++ _)


  def sendReady: DslF[F, Unit] = flow {
    if (!ready) {
      suspend(stream.write("READY".getBytes())) ++ delay(1.second) ++ eval(println("sent ready")) ++ sendReady
    } else unit
  }
}