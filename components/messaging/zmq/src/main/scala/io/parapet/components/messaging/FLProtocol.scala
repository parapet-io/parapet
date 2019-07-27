package io.parapet.components.messaging

import java.util.UUID

import com.typesafe.scalalogging.StrictLogging
import io.parapet.components.messaging.FLProtocol._
import io.parapet.components.messaging.api.ErrorCodes
import io.parapet.components.messaging.api.FLProtocolApi._
import io.parapet.components.messaging.Utils._
import io.parapet.components.messaging.api.MessagingApi._
import io.parapet.components.messaging.api.HeartbeatAPI.{Ping, Pong}
import io.parapet.core.Dsl.DslF
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.{Encoder, Event, Process, ProcessRef}
import org.zeromq.ZMQ.Poller
import org.zeromq.{SocketType, ZContext, ZMQ, ZMsg}

import scala.collection.JavaConverters._

import scala.collection.mutable
import scala.util.Try


/**
  * Brokerless reliable request-reply protocol that supports  multiple clients.
  * Implements the Freelance Protocol at http://rfc.zeromq.org/spec:10
  *
  * Client will receive responses in `send-order`, i.e. if a client sent three requests: req(1), req(2) and req(3)
  * it will receive responses in the following order: res(1), res(2), res(3).
  * Client can be synchronous or asynchronous, however the second case is slightly harder to implement
  * since client will have to equip it's request/response events with `correlation id` and do extra work to
  * match request/response pairs.
  *
  * @param encoder the encoder
  * @tparam F effect type
  */
class FLProtocol[F[_]](encoder: Encoder) extends Process[F] with StrictLogging {

  import dsl._

  private lazy val zmqContext = new ZContext(1)

  private lazy val control = zmqContext.createSocket(SocketType.PAIR) //  Socket for control messages


  override def handle: Receive = {
    case Start =>
      eval(control.bind(CONTROL_ADDR)) ++ register(ref, new Agent[F](zmqContext, encoder))
    case e@(_: Connect | _: Request) =>
      withSender { sender =>
        tryEval[F, Boolean](Try {
          val msg = new ZMsg
          msg.add(sender.toString())
          msg.add(encoder.write(e))
          msg.send(control, true)
        }, _ => unit, err => failure(err) ~> sender)
      }

    case Stop => Utils.close(zmqContext)
  }

}

object FLProtocol {

  def apply[F[_]](encoder: Encoder): Process[F] = new FLProtocol(encoder)

  // endpoint - Server identity/endpoint
  class Server(val endpoint: String, encoder: Encoder) {

    private var _alive = true // server is alive by default

    private var pingAt = System.currentTimeMillis + PING_INTERVAL //  Next ping at this time

    private var expires = System.currentTimeMillis + SERVER_TTL //  Expires at this time

    def expired: Boolean = System.currentTimeMillis() >= expires

    def destroy(): Unit = {
    }

    def updatePingInterval(): Unit = {
      pingAt = System.currentTimeMillis + PING_INTERVAL
    }

    def updateExpires(): Unit = {
      expires = System.currentTimeMillis + SERVER_TTL
    }

    def enable(): Unit = {
      _alive = true
    }

    def disable(): Unit = {
      _alive = false
    }

    def alive: Boolean = _alive

    // may fail if zmq context was interrupted
    def ping(socket: ZMQ.Socket): Unit = {
      if (System.currentTimeMillis >= pingAt) {
        val ping = new ZMsg
        ping.add(endpoint)
        ping.add("") // request id
        ping.add(encoder.write(Request(Ping)))
        ping.send(socket, true)
        pingAt = System.currentTimeMillis + PING_INTERVAL
      }
    }

    def tickless(tickless: Long): Long = Math.min(tickless, pingAt)
  }


  class Agent[F[_]](zmqContext: ZContext, encoder: Encoder) extends Process[F] with StrictLogging {

    import dsl._

    private lazy val router = zmqContext.createSocket(SocketType.ROUTER) //  Socket to talk to servers
    private lazy val control = zmqContext.createSocket(SocketType.PAIR) // Socket to talk to application
    private lazy val poller = zmqContext.createPoller(2)

    private val CONTROL_POLLIN = 0
    private val ROUTER_POLLIN = 1

    private val servers = new java.util.LinkedHashMap[String, Server]() //  Servers we've connected to

    // requests not yet sent
    private val pendingRequests = scala.collection.mutable.Queue[PendingRequest]()
    // requests sent to some server
    private val activeRequests = mutable.Map[String, PendingRequest]()

    def connect(endpoint: String): DslF[F, Unit] = eval {
      router.connect(endpoint)
      val server = new Server(endpoint, encoder)
      servers.put(endpoint, server)
      server.updatePingInterval()
      server.updateExpires()
    }

    private def init: DslF[F, Unit] = eval {
      control.connect(CONTROL_ADDR)
      poller.register(control, Poller.POLLIN)
      poller.register(router, Poller.POLLIN)
    }

    // receives control events from `control` socket
    private val controlFlow: DslF[F, Unit] = flow {

      def recvCtrlMsg: (ProcessRef, Event) = {
        val msg = ZMsg.recvMsg(control)
        val res = (ProcessRef(msg.popString()), encoder.read(msg.pop().getData))
        msg.destroy()
        res
      }

      if (poller.pollin(CONTROL_POLLIN)) {
        evalWith(recvCtrlMsg) {
          case (_, Connect(endpoint)) => connect(endpoint)
          case (sender, Request(data)) =>
            // todo: check the queue size and send an error to the sender if it's full
            // this may happen if there is no alive servers
            eval(pendingRequests.enqueue(PendingRequest(sender, data)))
        }
      } else unit

    }

    // receive server responses
    private val routerFlow: DslF[F, Unit] = flow {
      if (poller.pollin(ROUTER_POLLIN)) {
        evalWith(Try(ZMsg.recvMsg(router)).toOption) {
          case Some(reply) =>
            //  Frame 0 is server that replied, identity
            val endpoint = reply.popString()
            // Frame 1 is request id
            val reqId = Option(reply.popString())
            // Frame 2 is the actual response sent by server
            val res = Try(encoder.read(reply.pop().getData))
            reply.destroy()
            updateServerState(servers.get(endpoint)) ++
              ((reqId, res) match {
                case (None, _) =>
                  eval(logger.error(s"server: $endpoint sent invalid message: request id is missing"))
                case (Some(id), scala.util.Failure(err)) =>
                  processReply(id,
                    Failure(s"FL agent failed to decode response sent by server: $endpoint. Error: ${err.getMessage}",
                      ErrorCodes.EncodingError))
                case (Some(_), scala.util.Success(Success(Pong))) => unit
                case (Some(id), scala.util.Success(value)) => processReply(id, value)
              })
        }

      } else unit
    }

    private val sendReqFlow: DslF[F, Unit] = flow {
      pendingRequests.headOption match {
        case Some(req) =>
          if (req.expired) {
            // request expired, drop from the queue
            eval(pendingRequests.dequeue()) ++ Failure("request expired", ErrorCodes.RequestExpiredError) ~> req.sender
          } else {
            eval(disableExpiredServers()) ++
              (findActiveServer match {
                case Some(server) =>
                  eval {
                    pendingRequests.dequeue()
                    val reqId = UUID.randomUUID().toString
                    activeRequests.put(reqId, req)
                    sendToServer(server.endpoint, reqId, req.data)
                  }
                case None => unit // wait for active servers
              })
          }
        case None => unit // no requests
      }
    }


    private def loop: DslF[F, Unit] = {
      def step: DslF[F, Unit] = flow {
        val tickless = calcTickless
        evalWith(poller.poll(tickless - System.currentTimeMillis())) {
          case n if n <= -1 => unit
          case _ =>
            controlFlow ++
              routerFlow ++
              sendReqFlow ++
              deleteExpiredRequests ++
              sendPing ++
              step
        }

      }

      step
    }

    override def handle: Receive = {
      case Start => init ++ loop
    }

    private val deleteExpiredRequests: DslF[F, Unit] = {
      evalWith {
        val expiredEvents = activeRequests.filter {
          case (_, req) if req.expired => true
          case _ => false
        }
        activeRequests --= expiredEvents.keys
        expiredEvents
      } { expiredEvents =>
        if (expiredEvents.isEmpty) {
          unit
        } else {
          eval(logger.debug(s"events: ${expiredEvents.keys} expired")) ++
            expiredEvents.values.map(req => Failure("request expired", ErrorCodes.RequestExpiredError) ~> req.sender)
              .fold(unit)(_ ++ _)
        }
      }
    }

    private def sendToServer(endpoint: String, reqId: String, e: Event): Unit = {
      val msg = new ZMsg
      msg.add(endpoint)
      msg.add(reqId)
      msg.add(encoder.write(Request(e)))
      msg.send(router, true)
    }

    private def processReply(reqId: String, reply: Event): DslF[F, Unit] = {
      evalWith(activeRequests.remove(reqId)) {
        case Some(req) =>
          if (req.expired) Failure(s"request expired", ErrorCodes.RequestExpiredError) ~> req.sender
          else reply ~> req.sender
        case None => eval(logger.debug(s"unknown or stale request id=$reqId"))
      }
    }

    private def disableExpiredServers(): Unit = {
      servers.values.forEach { srv =>
        if (srv.expired) {
          logger.debug(s"server ${srv.endpoint} has been expired")
          srv.disable()
        }
      }
    }

    private def findActiveServer: Option[Server] = {
      servers.values.asScala.collectFirst {
        case s if s.alive => s
      }
    }

    private def updateServerState(server: Server): DslF[F, Unit] = eval {
      if (!server.alive) {
        server.enable()
      }
      server.updatePingInterval()
      server.updateExpires()
    }

    private def calcTickless: Long = {
      //  Calculate tickless timer, up to 1 hour
      var tickless = System.currentTimeMillis() + 1000 * 3600

      if (activeRequests.nonEmpty) {
        tickless = Math.min(tickless, activeRequests.values.minBy(_.expires).expires)
      }

      if (!servers.isEmpty) {
        tickless = servers.values.asScala.map(_.tickless(tickless)).min
      }
      tickless
    }

    private val sendPing: DslF[F, Unit] = eval {
      //  Send heartbeats to idle servers if needed
      servers.values.forEach { s =>
        Try(s.ping(router)) match {
          case scala.util.Failure(err) => logger.error(s"FL agent failed to send Ping to $s", err)
          case _ =>
        }
      }
    }

  }

  private val CONTROL_ADDR = "inproc://control"

  //  If not a single service replies within this time, give up
  private val GLOBAL_TIMEOUT = 2500
  //  PING interval for servers we think are alive
  private val PING_INTERVAL = 2000 //  msecs

  //  Server considered dead if silent for this long
  private val SERVER_TTL = 6000

  case class PendingRequest(sender: ProcessRef, data: Event) {
    // timeout request can be in pending queue
    val expires: Long = System.currentTimeMillis() + GLOBAL_TIMEOUT

    def expired: Boolean = System.currentTimeMillis() >= expires

    def resetExpirationTime: PendingRequest = PendingRequest(sender, data)
  }

}