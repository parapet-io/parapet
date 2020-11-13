package io.parapet.core.processes.net

import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.{Encoder, ProcessRef}
import org.zeromq.{SocketType, ZContext}

class AsyncServer[F[_]](override val ref: ProcessRef, address: String, sink: ProcessRef, encoder: Encoder) extends io.parapet.core.Process[F] {

  import dsl._

  private lazy val zmqContext = new ZContext(1)
  private lazy val server = zmqContext.createSocket(SocketType.ROUTER)

  private def loop: DslF[F, Unit] = flow {
    eval{
      val clientId = server.recvStr()
      val data = server.recv()
      data
    }.map(encoder.read).flatMap(e => e ~> sink) ++ loop
  }

  override def handle: Receive = {
    case Start => eval {
      server.bind(address)
    } ++ loop

    case Stop => eval {
      server.close()
      zmqContext.close()
    }

  }
}

object AsyncServer {
  def apply[F[_]](ref: ProcessRef, address: String, sink: ProcessRef, encoder: Encoder): AsyncServer[F] =
    new AsyncServer(ref, address, sink, encoder)
}