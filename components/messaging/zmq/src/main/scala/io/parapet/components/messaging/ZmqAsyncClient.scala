package io.parapet.components.messaging

import java.util.UUID

import com.typesafe.scalalogging.StrictLogging
import io.parapet.components.messaging.Utils._
import io.parapet.components.messaging.api.MessagingApi._
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.exceptions.UninitializedProcessException
import io.parapet.core.{Encoder, Event, Process}
import org.zeromq.{SocketType, ZContext, ZMsg}

import scala.language.higherKinds
import scala.util.Try

// todo implement true async client
class ZmqAsyncClient[F[_]](address: String,
                           encoder: Encoder) extends Process[F] with StrictLogging {

  import dsl._

  private lazy val zmqContext = new ZContext(1)
  private lazy val socket = zmqContext.createSocket(SocketType.DEALER)

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
          val msg = new ZMsg()
          val data = encoder.write(req)
          msg.add(UUID.randomUUID().toString)
          msg.add(data)
          msg.send(socket, true)
        }
      } {
        case scala.util.Success(_) =>
          evalWith(Try(ZMsg.recvMsg(socket, 0)).toOption) {
            case Some(res) =>
              res.popString() // ignore reqId
              Utils.tryEval[F, Event](Try(encoder.read(res.pop().getData)), res => withSender(res ~> _),
                err => withSender(sendFailure(_, err))
              )
            case None => {
              eval(logger.debug("zmq context was interrupted"))
            }
          }

        case scala.util.Failure(err) => withSender(sendFailure(_, err))
      }

  }

  override val handle: Receive = uninitialized
}

object ZmqAsyncClient {

  def apply[F[_]](address: String, encoder: Encoder): Process[F] =
    new ZmqAsyncClient(address, encoder)

}
