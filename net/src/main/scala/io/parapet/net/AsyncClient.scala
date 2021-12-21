package io.parapet.net

import cats.implicits.toFunctorOps
import io.parapet.ProcessRef
import io.parapet.core.Dsl.DslF
import io.parapet.core.Events.{Start, Stop}
import io.parapet.core.api.Cmd.netClient
import io.parapet.net.AsyncClient.Options
import org.slf4j.LoggerFactory
import org.zeromq.{SocketType, ZContext, ZMQ}

import scala.concurrent.duration._

class AsyncClient[F[_]](override val ref: ProcessRef,
                        clientId: String,
                        address: Address,
                        zmqContext: ZContext,
                        opts: Options) extends io.parapet.core.Process[F] {

  import dsl._

  private lazy val client = zmqContext.createSocket(SocketType.DEALER)
  private val logger = LoggerFactory.getLogger(ref.value)

  private val info: String = s"client[ref=$ref, id=$clientId, address=$address]"

  override def handle: Receive = {
    case Start =>
      eval {
        client
        client.setIdentity(clientId.getBytes(ZMQ.CHARSET))
        client.setReceiveTimeOut(opts.receiveTimeOut.toMillis.intValue())
        client.setSndHWM(opts.sndHWM)
        client.connect(address.value)
        logger.debug(s"client[id=$clientId] has been connected to $address")
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
            _ <- devLog("wait for response")
            msg <- eval(client.recv())
            _ <- devLog(
              s"[$address] response received: " +
                s"'${new String(Option(msg).getOrElse(Array.empty))}'. send to $repChan")
            _ <- netClient.Rep(Option(msg)) ~> repChan
          } yield ()
        case None => unit
      }
      blocking {
        devLog(s"$info send message") ++
          eval(client.send(data, 0)).void
            .handleError(err => eval(logger.error(s"$info failed to send a message", err))) ++
          waitForRep.handleError(err => eval(logger.error(s"$info failed to receive a reply", err)))
      }
  }

  private def devLog(msg: => String): DslF[F, Unit] = {
    if (context.devMode) {
      eval(logger.debug(msg))
    } else unit
  }
}

object AsyncClient {

  case class Options(receiveTimeOut: FiniteDuration, sndHWM: Int) {
    def withSndHWM(value: Int): Options = this.copy(sndHWM = value)
  }

  val defaultOpts: Options = Options(5.seconds, 1000)

  def apply[F[_]](ref: ProcessRef,
                  zmqContext: ZContext,
                  clientId: String,
                  address: Address,
                  opts: Options = defaultOpts): AsyncClient[F] =
    new AsyncClient(ref, clientId, address, zmqContext, opts)
}
