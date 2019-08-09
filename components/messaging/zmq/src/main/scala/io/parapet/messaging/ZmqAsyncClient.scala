package io.parapet.messaging

import java.util.UUID

import cats.effect.Concurrent
import cats.syntax.functor._
import com.typesafe.scalalogging.StrictLogging
import io.parapet.core.Dsl.DslF
import io.parapet.core.Encoder.EncodingException
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.exceptions.UninitializedProcessException
import io.parapet.core.{Encoder, Event, Process, ProcessRef, Queue}
import io.parapet.messaging.Utils._
import io.parapet.messaging.ZmqAsyncClient.{PendingRequest, Worker}
import io.parapet.messaging.api.ErrorCodes
import io.parapet.messaging.api.MessagingApi._
import io.parapet.messaging.api.ServerAPI.Envelope
import org.zeromq.{SocketType, ZContext, ZMQ, ZMsg}

import scala.collection.mutable
import scala.language.higherKinds
import scala.util.Try

class ZmqAsyncClient[F[_]](address: String,
                           encoder: Encoder, requestQueue: Queue[F, PendingRequest]) extends Process[F] with StrictLogging {

  import dsl._

  private val clientId = UUID.randomUUID().toString

  private lazy val zmqContext = new ZContext(1)
  private lazy val socket = zmqContext.createSocket(SocketType.DEALER)

  private val pendingRequests = mutable.Map[String, ProcessRef]()

  private lazy val worker = new Worker[F](ref, zmqContext, address, requestQueue, encoder)

  private val init = eval {
    socket.setIdentity(clientId.getBytes(ZMQ.CHARSET))
    socket.connect(address)
  }

  private def uninitialized: Receive = {
    case Start => init ++ switch(ready) ++ register(ref, worker)
    case Stop => unit
    case _ => eval(throw UninitializedProcessException("zmq sync client isn't initialized"))
  }


  private def ready: Receive = {
    case Stop => Utils.close(zmqContext)

    case req: Request =>
      withSender { sender =>
        evalWith {
          val reqId = UUID.randomUUID().toString
          pendingRequests += (reqId -> sender)
          val pReq = PendingRequest(reqId, req)
          pReq

        } { pReq =>
          suspend(requestQueue.enqueue(pReq))
        }
      }

    case Envelope(requestId, event) =>
      evalWith(pendingRequests.remove(requestId)) {
        case Some(sender) => event ~> sender
        case None => eval(logger.error(s"unknown request id: $requestId"))
      }
  }

  override val handle: Receive = uninitialized
}

object ZmqAsyncClient {

  class Worker[F[_]](parent: ProcessRef,
                     zmqContext: ZContext,
                     address: String,
                     requestQueue: Queue[F, PendingRequest],
                     encoder: Encoder) extends Process[F] with StrictLogging {

    import dsl._

    private lazy val socket = zmqContext.createSocket(SocketType.DEALER)

    private def receiveRequest: DslF[F, Unit] = {
      suspendWith(requestQueue.dequeue) {
        case PendingRequest(reqId, req) =>
          evalWith {
            Try {
              val msg = new ZMsg()
              val data = encoder.write(req)
              msg.add(reqId)
              msg.add(data)
              msg.send(socket, true)
            }
          } {
            case scala.util.Success(_) => receiveResponse(parent) ++ receiveRequest
            case scala.util.Failure(err: EncodingException) =>
              sendFailure[F](parent, err) ++ receiveRequest
            case scala.util.Failure(err) => sendFailure(parent, err) // zmq context was interrupted
          }
      }

    }

    def receiveResponse(sender: ProcessRef): DslF[F, Unit] = {
      evalWith(Try(ZMsg.recvMsg(socket, 0)).toOption) {
        case Some(zMsg) =>
          val reqId = zMsg.popString()
          Try(encoder.read(zMsg.pop().getData)) match {
            case scala.util.Success(res) => Envelope(reqId, res) ~> sender
            case scala.util.Failure(err) =>
              Envelope(reqId, Failure(err.getMessage, ErrorCodes.EncodingError)) ~> sender
          }
        case None => eval(logger.debug("zmq context was interrupted"))
      }
    }

    override def handle: Receive = {
      case Start => eval {
        socket.connect(address)
      } ++ receiveRequest
    }
  }


  def apply[F[_] : Concurrent](address: String, encoder: Encoder): F[Process[F]] = {
    for {
      queue <- Queue.unbounded[F, PendingRequest]
    } yield new ZmqAsyncClient(address, encoder, queue)
  }

  private case class PendingRequest(id: String, event: Event)

}
