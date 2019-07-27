package io.parapet.components.messaging

import io.parapet.components.messaging.Utils._
import io.parapet.components.messaging.api.MessagingApi._
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.exceptions.UninitializedProcessException
import io.parapet.core.{Encoder, Process}
import org.zeromq.{SocketType, ZContext, ZMsg}

import scala.language.higherKinds
import scala.util.Try

/**
  * This client is based on request-reply socket pattern and supports
  * strictly synchronous request-reply dialog.
  *
  * The client issues zmq_send() and then zmq_recv() in one step, i.e. the client blocks asynchronously
  * until it receives a response from server.
  *
  * When a process sends an event to this client, it's added to the client process event queue.
  * Once client receives a response from server it will pull out the next available event from it's queue and
  * send it to the server then repeat send-recv cycle.
  *
  * From ZMQ documentation
  *
  * The REQ socket sends, to the network, an empty delimiter frame in front of the message data.
  * REQ sockets are synchronous.
  * REQ sockets always send one request and then wait for one reply.
  * REQ sockets talk to one peer at a time. If you connect a REQ socket to multiple peers,
  * requests are distributed to and replies expected from each peer one turn at a time.
  *
  * @param address a string consisting of a transport :// followed by an address
  * @param encoder encoder to convert events to byte array and vise-versa
  */
class ZmqSyncClient[F[_]](address: String,
                          encoder: Encoder) extends Process[F] {

  import dsl._

  private lazy val zmqContext = new ZContext(1)
  private lazy val socket = zmqContext.createSocket(SocketType.REQ)

  private val init = eval {
    socket.connect(address)
  }

  private def uninitialized: Receive = {
    case Start => init ++ switch(ready)
    case Stop => unit
    case _ => eval(throw UninitializedProcessException("zmq sync client isn't initialized"))
  }


  private def ready: Receive = {
    case Stop => Utils.close(zmqContext)

    case req: Request =>
      evalWith {
        Try {
          val data = encoder.write(req)
          socket.send(data, 0)
        }
      } {
        case scala.util.Success(_) =>
          evalWith {
            Try {
              val data = socket.recv(0)
              encoder.read(data)
            }
          } {
            case scala.util.Success(res) => withSender(res ~> _)
            case scala.util.Failure(err) => withSender(sendFailure(_, err))
          }
        case scala.util.Failure(err) => withSender(sendFailure(_, err))
      }

  }

  override val handle: Receive = uninitialized

}

object ZmqSyncClient {

  def apply[F[_]](address: String, encoder: Encoder): Process[F] =
    new ZmqSyncClient(address, encoder)

}