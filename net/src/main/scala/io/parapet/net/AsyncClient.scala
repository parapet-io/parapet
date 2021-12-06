package io.parapet.net

import cats.implicits.toFunctorOps
import io.parapet.ProcessRef
import io.parapet.core.Events.{Start, Stop}
import io.parapet.core.api.Cmd.netClient
import org.slf4j.LoggerFactory
import org.zeromq.{SocketType, ZContext, ZMQ}
import scala.concurrent.duration._

class AsyncClient[F[_]](override val ref: ProcessRef,
                        clientId: String,
                        address: String,
                        receiveTimeOut: FiniteDuration) extends io.parapet.core.Process[F] {

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
        logger.debug(s"client[id=$clientId] has been connected to $address")
        client.setReceiveTimeOut(receiveTimeOut.toMillis.intValue())
      }

    case Stop =>
      eval {
        client.close()
        zmqContext.close()
      }

    case netClient.Send(data, replyOpt) =>
      val waitForRep = replyOpt match {
        case Some(repChan) =>
          for {
            _ <- eval(logger.debug("wait for response"))
            msg <- eval(client.recv())
            _ <- eval(
              logger.debug(
                s"[$address] response received: '${new String(Option(msg).getOrElse(Array.empty))}'. send to $repChan"))
            _ <- netClient.Rep(Option(msg)) ~> repChan
          } yield ()
        case None => unit
      }

      eval(logger.debug(s"$info send message")) ++
        eval(client.send(data, 0)).void
          .handleError(err => eval(logger.error(s"$info failed to send a message", err))) ++
        waitForRep.handleError(err => eval(logger.error(s"$info failed to receive a reply", err)))

  }
}

object AsyncClient {

  def apply[F[_]](ref: ProcessRef,
                  clientId: String,
                  address: String,
                  receiveTimeOut: FiniteDuration = 5.seconds): AsyncClient[F] =
    new AsyncClient(ref, clientId, address, receiveTimeOut)
}
