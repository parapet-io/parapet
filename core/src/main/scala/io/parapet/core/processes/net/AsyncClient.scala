package io.parapet.core.processes.net

import cats.implicits.toFunctorOps
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events.{Start, Stop}
import io.parapet.core.api.Event
import io.parapet.core.processes.net.AsyncClient._
import io.parapet.core.{Encoder, ProcessRef}
import org.slf4j.LoggerFactory
import org.zeromq.{SocketType, ZContext, ZMQ}

class AsyncClient[F[_]](override val ref: ProcessRef, clientId: String, address: String, clientRef: Option[ProcessRef] = None)
    extends io.parapet.core.Process[F] {

  import dsl._

  private lazy val zmqContext = new ZContext(1)
  private lazy val client = zmqContext.createSocket(SocketType.DEALER)
  private val logger = LoggerFactory.getLogger(ref.value)

  private val info: String = s"client[ref=$ref, id=$clientId, address=$address]"

  override def handle: Receive = {
    case Start =>
      eval {
        client
        client.setIdentity(clientId.getBytes(ZMQ.CHARSET))
        client.connect(address)
        logger.debug(s"client[id=$clientId] connected to $address")
      }

    case Stop =>
      eval {
        client.close()
        zmqContext.close()
      }

    case Send(data) =>
      eval(client.send(data, 0)).void
        .handleError(err => eval(logger.error(s"$info failed to send message", err)))

    case Recv =>
      for {
        msg <- eval(client.recv())
        _ <- Rep(msg) ~> clientRef.get
      } yield ()
  }
}

object AsyncClient {

  sealed trait API extends Event
  case class Send(data: Array[Byte]) extends API
  case class Rep(data: Array[Byte]) extends API
  case object Recv extends Event

  def apply[F[_]](ref: ProcessRef, clientId: String, address: String): AsyncClient[F] =
    new AsyncClient(ref, clientId, address)
}
