package io.parapet.core.processes.net

import com.typesafe.scalalogging.Logger
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.{Encoder, ProcessRef}
import org.slf4j.LoggerFactory
import org.zeromq.{SocketType, ZContext, ZMQ}

class AsyncClient[F[_]](override val ref: ProcessRef, clientId: String, address: String, encoder: Encoder)
    extends io.parapet.core.Process[F] {

  import dsl._

  private lazy val zmqContext = new ZContext(1)
  private lazy val client = zmqContext.createSocket(SocketType.DEALER)
  private val logger = LoggerFactory.getLogger(ref.value)

  override def handle: Receive = {
    case Start =>
      eval {
        client.setIdentity(clientId.getBytes(ZMQ.CHARSET))
        client.connect(address)
        logger.debug(s"client[id=$clientId] connected to $address")
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
  def apply[F[_]](ref: ProcessRef, clientId: String, address: String, encoder: Encoder): AsyncClient[F] =
    new AsyncClient(ref, clientId, address, encoder)
}
