package io.parapet.core.processes.net

import io.parapet.core.Dsl.DslF
import io.parapet.core.Events.{Start, Stop}
import io.parapet.core.ProcessRef
import io.parapet.core.api.Event
import org.slf4j.LoggerFactory
import org.zeromq.{SocketType, ZContext, ZMQException, ZMsg}
import zmq.ZError

class AsyncServer[F[_]](override val ref: ProcessRef, address: String, sink: ProcessRef)
    extends io.parapet.core.Process[F] {

  import AsyncServer._
  import dsl._

  private lazy val zmqContext = new ZContext(1)
  private lazy val server = zmqContext.createSocket(SocketType.ROUTER)
  private val logger = LoggerFactory.getLogger(ref.value)
  private var _stoped = false

  private def init = eval {
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
  }

  private def step: DslF[F, Either[Throwable, Message]] = {
    if (_stoped) {
      eval {
        logger.info("server is not running. stop receive loop")
        Left(new RuntimeException("server is not running")).withRight[Message]
      }
    } else {
      eval {
        val clientId = server.recvStr()
        val msgBytes = server.recv()
        logger.debug(s"server $ref received message from client: $clientId")
        Right(Message(clientId, msgBytes)).withLeft[Throwable]
      }.handleError(e => eval(Left(e).withRight[Message])).flatMap {
        case Right(msg) => msg ~> sink ++ step
        case Left(err: org.zeromq.ZMQException) if err.getErrorCode == ZError.ETERM =>
          eval(logger.error("zmq context has been terminated. stop receive loop", err)) ++
            eval(Left(err).withRight[Message])
        case Left(err) => eval(logger.error("net server failed to process msg", err)) ++ step
      }
    }
  }

  private def loop: DslF[F, Unit] = flow {
    step.map(_ => ())
  }

  override def handle: Receive = {
    case Start => init ++ fork(loop)

    case Send(clientId, data) =>
      eval {
        if (_stoped) {
          eval(logger.error("server is not running"))
        } else {
          logger.debug(s"send message to $clientId")
          val msg = new ZMsg()
          msg.add(clientId)
          msg.add(data)
          msg.send(server)
        }
      }

    case Stop =>
      eval {
        _stoped = true
        server.close()
        zmqContext.close()
      }

  }
}

object AsyncServer {

  // API
  sealed trait Api extends Event

  case class Send(clientId: String, data: Array[Byte]) extends Api

  case class Message(clientId: String, data: Array[Byte]) extends Api

  def apply[F[_]](ref: ProcessRef, address: String, sink: ProcessRef): AsyncServer[F] =
    new AsyncServer(ref, address, sink)
}
