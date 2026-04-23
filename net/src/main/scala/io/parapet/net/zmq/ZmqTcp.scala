package io.parapet.net.zmq

import io.parapet.effect.Effect
import io.parapet.net.{NetworkAddress, ReceivedFrame, RequestResponseClient, RequestResponseServer, TransportProtocol}
import org.zeromq.{SocketType, ZContext}

import java.util.Base64
import java.util.concurrent.ConcurrentHashMap

final case class ZmqTcpClientConfig(
    remote: NetworkAddress,
    receiveTimeoutMs: Int = 5000,
    ioThreads: Int = 1
):
  require(remote.protocol == TransportProtocol.Tcp, "ZMQ TCP client requires a tcp address")

final case class ZmqTcpServerConfig(
    bind: NetworkAddress,
    receiveTimeoutMs: Int = 250,
    ioThreads: Int = 1
):
  require(bind.protocol == TransportProtocol.Tcp, "ZMQ TCP server requires a tcp address")

final class ZmqTcpClient[F[_]](config: ZmqTcpClientConfig)(using effect: Effect[F]) extends RequestResponseClient[F]:
  private val context = new ZContext(config.ioThreads)
  private val socket = context.createSocket(SocketType.REQ)

  socket.setReceiveTimeOut(config.receiveTimeoutMs)
  socket.connect(config.remote.uri)

  def request(payload: Array[Byte]): F[Option[Array[Byte]]] =
    effect.blocking {
      if !socket.send(payload, 0) then
        throw new IllegalStateException(s"failed to send a request to ${config.remote.uri}")
      Option(socket.recv(0)).map(_.clone())
    }

  def close: F[Unit] =
    effect.delay {
      socket.close()
      context.close()
      ()
    }

final class ZmqTcpServer[F[_]](config: ZmqTcpServerConfig)(using effect: Effect[F]) extends RequestResponseServer[F]:
  private val context = new ZContext(config.ioThreads)
  private val socket = context.createSocket(SocketType.ROUTER)
  private val routes = new ConcurrentHashMap[String, Array[Byte]]()

  socket.setReceiveTimeOut(config.receiveTimeoutMs)
  socket.bind(config.bind.uri)

  def receive: F[Option[ReceivedFrame]] =
    effect.blocking {
      val route = socket.recv(0)
      if route == null then None
      else
        val next = socket.recv(0)
        val payload =
          if next == null then Array.emptyByteArray
          else if next.isEmpty then Option(socket.recv(0)).getOrElse(Array.emptyByteArray)
          else next

        val clientId = Base64.getEncoder.encodeToString(route)
        routes.put(clientId, route.clone())
        Some(ReceivedFrame(clientId, payload.clone()))
    }

  def reply(clientId: String, payload: Array[Byte]): F[Unit] =
    effect.blocking {
      val route = Option(routes.get(clientId))
        .getOrElse(throw new IllegalArgumentException(s"unknown client route: $clientId"))

      if !socket.sendMore(route) then
        throw new IllegalStateException(s"failed to route reply to client $clientId")
      if !socket.sendMore(Array.emptyByteArray) then
        throw new IllegalStateException(s"failed to send ZMQ delimiter to client $clientId")
      if !socket.send(payload, 0) then
        throw new IllegalStateException(s"failed to send reply payload to client $clientId")
      ()
    }

  def close: F[Unit] =
    effect.delay {
      socket.close()
      context.close()
      ()
    }
