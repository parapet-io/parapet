package io.parapet.net.transport.zmq

import io.parapet.effect.Effect
import io.parapet.effect.Resource
import io.parapet.net.{Endpoint, TransportProtocol}
import io.parapet.net.transport.*
import org.zeromq.{SocketType, ZContext, ZMQException}

import java.util.Base64
import java.util.LinkedHashMap
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer

final case class ZmqTcpServerConfig(
    bind: Endpoint,
    receiveTimeoutMs: Int = 250,
    ioThreads: Int = 1,
    peerSocketType: SocketType = SocketType.REQ,
    maxRoutes: Int = 4096
):
  require(bind.protocol == TransportProtocol.Tcp, "ZMQ TCP server requires a tcp endpoint")
  require(
    peerSocketType == SocketType.REQ || peerSocketType == SocketType.DEALER,
    "ZMQ TCP server supports REQ and DEALER peer socket types"
  )
  require(maxRoutes > 0, "maxRoutes must be positive")

final class ZmqTcpServer[F[_]] private (config: ZmqTcpServerConfig)(using effect: Effect[F]) extends ServerTransport[F]:
  import ZmqTcpServer.RouteEntry

  private val context = new ZContext(config.ioThreads)
  private val socket  = context.createSocket(SocketType.ROUTER)
  private val closed  = new AtomicBoolean(false)

  private val routes = new LinkedHashMap[String, RouteEntry](16, 0.75f, true):
    override protected def removeEldestEntry(eldest: java.util.Map.Entry[String, RouteEntry]): Boolean =
      size() > config.maxRoutes

  socket.setReceiveTimeOut(config.receiveTimeoutMs)
  socket.setLinger(0)
  socket.bind(config.bind.uri)

  def receive: F[ReceiveResult[RoutedMessage]] =
    effect.blocking {
      if closed.get() then ReceiveResult.Failed(TransportError.Closed("receive"))
      else
        try receiveOnce()
        catch
          case _: ZMQException if closed.get() =>
            ReceiveResult.Failed(TransportError.Closed("receive"))
          case error: ZMQException =>
            ReceiveResult.Failed(TransportError.Unexpected(error))
    }

  private def receiveOnce(): ReceiveResult[RoutedMessage] =
    val identity = socket.recv(0)
    if identity == null then ReceiveResult.Idle
    else
      val frames = ListBuffer.empty[Array[Byte]]
      while socket.hasReceiveMore do frames += socket.recv(0)

      bodyParts(frames.toVector) match
        case Left(error) =>
          ReceiveResult.Failed(error)
        case Right(body) =>
          val routingId = RoutingId(Base64.getEncoder.encodeToString(identity))
          routes.synchronized {
            routes.put(routingId.value, RouteEntry(identity.clone()))
          }
          ReceiveResult.Received(RoutedMessage(routingId, Message(body)))

  private def bodyParts(frames: Vector[Array[Byte]]): Either[TransportError, Vector[Array[Byte]]] =
    config.peerSocketType match
      case SocketType.REQ    => stripDelimiter(frames)
      case SocketType.DEALER => Right(frames)
      case other             => Left(TransportError.ProtocolViolation(s"unsupported ZMQ peer socket type: $other"))

  private def stripDelimiter(frames: Vector[Array[Byte]]): Either[TransportError, Vector[Array[Byte]]] =
    frames.headOption match
      case Some(delimiter) if delimiter.isEmpty => Right(frames.tail)
      case Some(_)                              =>
        Left(TransportError.ProtocolViolation("REQ peer message is missing empty delimiter frame"))
      case None =>
        Left(TransportError.ProtocolViolation("REQ peer message is missing delimiter and body frames"))

  def reply(routingId: RoutingId, message: Message): F[Either[TransportError, Unit]] =
    effect.blocking {
      if closed.get() then Left(TransportError.Closed("reply"))
      else
        routes.synchronized(Option(routes.get(routingId.value))) match
          case None =>
            Left(TransportError.UnknownRoute(routingId))
          case Some(entry) =>
            try sendReply(routingId, entry, message)
            catch
              case error: ZMQException =>
                Left(TransportError.Unexpected(error))
    }

  private def sendReply(
      routingId: RoutingId,
      entry: RouteEntry,
      message: Message
  ): Either[TransportError, Unit] =
    if !socket.sendMore(entry.identity) then
      Left(TransportError.SendFailed("reply", s"failed to route reply to ${routingId.value}"))
    else
      config.peerSocketType match
        case SocketType.REQ if !socket.sendMore(Array.emptyByteArray) =>
          Left(TransportError.SendFailed("reply", s"failed to send delimiter to ${routingId.value}"))
        case _ =>
          sendMessage(message, "reply")

  private def sendMessage(message: Message, operation: String): Either[TransportError, Unit] =
    val parts = if message.parts.isEmpty then Vector(Array.emptyByteArray) else message.parts
    var sent  = true
    parts.zipWithIndex.foreach { case (part, index) =>
      val last = index == parts.size - 1
      sent = sent && (if last then socket.send(part, 0) else socket.sendMore(part))
    }
    if sent then Right(()) else Left(TransportError.SendFailed(operation, "failed to send message body"))

  def close: F[Unit] =
    effect.delay {
      if closed.compareAndSet(false, true) then context.close()
    }

object ZmqTcpServer:
  def make[F[_]: Effect](config: ZmqTcpServerConfig): Resource[F, ServerTransport[F]] =
    val effect = Effect[F]
    Resource.make(effect.delay(new ZmqTcpServer[F](config)))(_.close)

  final private case class RouteEntry(identity: Array[Byte])
