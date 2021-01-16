package io.parapet.core.processes.net

import java.util.UUID

import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.{Encoder, ProcessRef}
import org.zeromq.{SocketType, ZContext, ZMQ}

class AsyncClient[F[_]](override val ref: ProcessRef, address: String, encoder: Encoder)
    extends io.parapet.core.Process[F] {

  import dsl._

  private val clientId = UUID.randomUUID().toString

  private lazy val zmqContext = new ZContext(1)
  private lazy val client = zmqContext.createSocket(SocketType.DEALER)

  override def handle: Receive = {
    case Start =>
      eval {
        client.setIdentity(clientId.getBytes(ZMQ.CHARSET))
        client.connect(address)
        println(s"client[ref=$ref, id=$clientId] connected")
      }

    case Stop =>
      eval {
        client.close()
        zmqContext.close()
      }

    case e =>
      eval {
        val data = encoder.write(e)
        client.send(data, 0)
      }
  }
}

object AsyncClient {
  def apply[F[_]](ref: ProcessRef, address: String, encoder: Encoder): AsyncClient[F] =
    new AsyncClient(ref, address, encoder)
}
