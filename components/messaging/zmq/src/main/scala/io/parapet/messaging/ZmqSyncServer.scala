package io.parapet.messaging

import cats.effect.Concurrent
import com.typesafe.scalalogging.StrictLogging
import io.parapet.messaging.ZmqSyncServer._
import io.parapet.messaging.api.ErrorCodes
import io.parapet.messaging.api.MessagingApi.{Failure, Request, Response, Success}
import io.parapet.core.Dsl.DslF
import io.parapet.core.Encoder.EncodingException
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.exceptions.UninitializedProcessException
import io.parapet.core.{Channel, Encoder, Event, Process, ProcessRef}
import org.zeromq.{SocketType, ZContext}

import scala.util.Try

/**
  * This server is based on request-reply socket pattern and supports
  * strictly synchronous request-reply dialog.
  * This process zmq_recv() and then zmq_send() in that order.
  * Doing any other sequence, e.g., sending two reply events in a row
  * will result in a Failure event sent to the sender.
  *
  * From ZMQ documentation:
  *
  * The REP socket reads and saves all identity frames up to and including the empty delimiter,
  * then passes the following frame or frames to the caller.
  * REP sockets are synchronous and talk to one peer at a time. If you connect a REP socket to multiple
  * peers, requests are read from peers in fair fashion, and replies are always sent to the same peer
  * that made the last request.
  *
  * @param address  a string consisting of a transport :// followed by an address
  * @param receiver a process that should handle incoming events and eventually send a response back to this server process.
  * @param encoder  encoder to convert events to byte array and vise-versa
  */
class ZmqSyncServer[F[_] : Concurrent]
(address: String, receiver: ProcessRef, encoder: Encoder) extends Process[F] with StrictLogging {

  import dsl._

  private lazy val zmqContext = new ZContext(1)
  private lazy val socket = zmqContext.createSocket(SocketType.REP)
  private lazy val ch = Channel[F] // req-rep channel to talk to `receiver`

  private val init = eval {
    socket.bind(address)
  } ++ register(ref, ch)

  private def uninitialized: Receive = {
    case Start => init ++ switch(ready) ++ Await ~> ref
    case Stop => unit
    case _ => eval(throw UninitializedProcessException("zmq sync server isn't initialized"))
  }

  private def ready: Receive = {
    case Stop =>
      eval(Try(socket.close())) ++ Utils.close(zmqContext)
    case Await =>
      evalWith {
        Try {
          val data = socket.recv(0)
          encoder.read(data)
        }
      } {
        case scala.util.Success(Request(event)) => ch.send(event, receiver, {
          case scala.util.Success(res) => sendResponse(Success(res))
          case scala.util.Failure(err) =>
            sendResponse(Failure(s"Receiver failed to handle request. Error: ${err.getMessage}", ErrorCodes.EventHandlingError))
        }) ++ Await ~> ref
        case scala.util.Failure(err) => err match {
          case ex: EncodingException => sendResponse(Failure(ex.getMessage, ErrorCodes.EncodingError)) ++ Await ~> ref
          case _ => eval(logger.error("server failed to receive request. ZMQ context was interrupted", err))
        }
      }
  }

  private def sendResponse(res: Response): DslF[F, Unit] =
    evalWith {
      Try {
        val data = encoder.write(res)
        socket.send(data, 0)
      }
    } {
      case scala.util.Success(_) => unit
      case scala.util.Failure(err: EncodingException) =>
        sendResponse(Failure(s"server failed to encode reply event received from receiver. Error: ${err.getMessage}",
          ErrorCodes.EncodingError))
      case scala.util.Failure(err) =>
        eval(logger.error("server failed to send request. ZMQ context was interrupted", err))
    }


  override val handle: Receive = uninitialized
}

object ZmqSyncServer {

  def apply[F[_] : Concurrent](address: String, receiver: ProcessRef, encoder: Encoder): ZmqSyncServer[F] =
    new ZmqSyncServer(address, receiver, encoder)

  private object Await extends Event

}
