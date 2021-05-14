package io.parapet.core.processes.net

import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.{Encoder, Event, ProcessRef}
import org.slf4j.LoggerFactory
import org.zeromq.{SocketType, ZContext, ZMQException, ZMsg}
import zmq.ZError

import java.nio.ByteBuffer

class AsyncServer[F[_]](override val ref: ProcessRef, address: String, sink: ProcessRef, encoder: Encoder)
    extends io.parapet.core.Process[F] {

  import AsyncServer._

  import dsl._

  private lazy val zmqContext = new ZContext(1)
  private lazy val server = zmqContext.createSocket(SocketType.ROUTER)
  private val logger = LoggerFactory.getLogger(ref.value)

  private val step0 = eval {
    val clientId = server.recvStr()
    val clientIdBytes = clientId.getBytes()
    val msgBytes = server.recv()
    val size = 4 + clientIdBytes.length + msgBytes.length
    val buf = ByteBuffer.allocate(size)
    buf.putInt(clientIdBytes.length)
    buf.put(clientIdBytes)
    buf.put(msgBytes)
    val data = new Array[Byte](size)
    buf.rewind()
    buf.get(data)
    val msg = encoder.read(data)
    logger.debug(s"received message = $msg from client: $clientId")
    msg
  }.flatMap(e => e ~> sink)

  def step: DslF[F, Unit] = {
    step0.handleError {
      case err: org.zeromq.ZMQException if err.getErrorCode == ZError.ETERM =>
        eval(logger.error("zmq context has been terminated", err)) ++ eval(throw err)
      case err => eval(logger.error("net server failed to process msg", err))
    }
  }

  private def loop: DslF[F, Unit] = flow {
    step ++ loop
  }

  override def handle: Receive = {
    case Start =>
      eval {
        try {
          server.bind(address)
          logger.debug(s"$ref server started on $address")
        } catch {
          case e: Exception =>
            e match {
              case zmqError: ZMQException if zmqError.getErrorCode == 48 => logger.error(s"address: '$address' in use")
              case _ => ()
            }
            throw e
        }

      } ++ fork(loop)

    case Send(clientId, data) =>
      eval {
        logger.debug(s"send message to $clientId")
        val msg = new ZMsg()
        msg.add(clientId)
        msg.add(data)
        msg.send(server)
      }

    case Stop =>
      eval {
        server.close()
        zmqContext.close()
      }

  }
}

object AsyncServer {

  // API
  sealed trait Api extends Event
  case class Send(clientId: String, data: Array[Byte]) extends Api

  def apply[F[_]](ref: ProcessRef, address: String, sink: ProcessRef, encoder: Encoder): AsyncServer[F] =
    new AsyncServer(ref, address, sink, encoder)
}
