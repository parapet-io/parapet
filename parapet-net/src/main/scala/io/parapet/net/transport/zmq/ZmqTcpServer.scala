package io.parapet.net.transport.zmq

import io.parapet.effect.Effect
import io.parapet.effect.Resource
import io.parapet.net.{Endpoint, TransportProtocol}
import io.parapet.net.transport.*
import org.zeromq.{SocketType, ZContext, ZMQ}

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ListBuffer

final case class ZmqTcpServerConfig(
    bind: Endpoint,
    receiveTimeoutMs: Int = 250,
    workerPollMs: Int = 20,
    ioThreads: Int = 1,
    routeTtlMs: Long = 30_000
):
  require(bind.protocol == TransportProtocol.Tcp, "ZMQ TCP server requires a tcp endpoint")
  require(workerPollMs > 0, "workerPollMs must be positive")
  require(routeTtlMs > 0, "routeTtlMs must be positive")

/** ROUTER-backed request/reply server transport.
  *
  * The ROUTER socket is owned by a single [[ZmqSocketWorker]]: `receive` drains messages the worker has polled, and
  * `reply` submits an explicit reply command to that worker. Routing ids are opaque, server-local handles; ZMQ identity
  * bytes stay inside the transport.
  */
final class ZmqTcpServer[F[_]] private (config: ZmqTcpServerConfig)(using effect: Effect[F]) extends ServerTransport[F]:
  import ZmqTcpServer.{Command, PeerEnvelope, RouteEntry}

  private val context = new ZContext(config.ioThreads)
  private val socket  = context.createSocket(SocketType.ROUTER)

  private val routes = new ConcurrentHashMap[String, RouteEntry]()

  socket.setReceiveTimeOut(config.workerPollMs)
  socket.setLinger(0)
  socket.bind(config.bind.uri)

  private val loop =
    new ZmqSocketWorker[Command, RoutedMessage](
      context,
      socket,
      readInbound,
      handleCommand,
      s"zmq-server-${config.bind.port}"
    )

  def receive: F[ReceiveResult[RoutedMessage]] =
    effect.blocking(loop.poll(config.receiveTimeoutMs))

  def reply(routingId: RoutingId, message: Message): F[Either[TransportError, Unit]] =
    effect.blocking(loop.submit(Command.Reply(routingId, message)))

  def close: F[Unit] =
    effect.delay(loop.close())

  private def handleCommand(sock: ZMQ.Socket, command: Command): Either[TransportError, Unit] =
    expireRoutes()
    command match
      case Command.Reply(routingId, message) =>
        takeRoute(routingId) match
          case None        => Left(TransportError.UnknownRoute(routingId))
          case Some(entry) => sendReply(sock, routingId, entry, message)

  private def readInbound(sock: ZMQ.Socket): ReceiveResult[RoutedMessage] =
    val identity = sock.recv(0)
    if identity == null then ReceiveResult.Idle
    else
      val frames = ListBuffer.empty[Array[Byte]]
      while sock.hasReceiveMore do frames += sock.recv(0)

      decodeEnvelope(frames.toVector) match
        case Left(error) =>
          ReceiveResult.Failed(error)
        case Right((envelope, frame)) =>
          decodeMessage(frame) match
            case Left(error) =>
              ReceiveResult.Failed(error)
            case Right(message) =>
              expireRoutes()
              val routingId = registerRoute(identity, envelope)
              ReceiveResult.Received(RoutedMessage(routingId, message))

  private def registerRoute(identity: Array[Byte], envelope: PeerEnvelope): RoutingId =
    val routingId = RoutingId(UUID.randomUUID().toString)
    routes.put(routingId.value, RouteEntry(identity.clone(), envelope, System.currentTimeMillis()))
    routingId

  private def takeRoute(routingId: RoutingId): Option[RouteEntry] =
    Option(routes.remove(routingId.value)).filterNot(isExpired)

  private def expireRoutes(): Unit =
    val iterator = routes.entrySet().iterator()
    while iterator.hasNext do
      val entry = iterator.next()
      if isExpired(entry.getValue) then iterator.remove()

  private def isExpired(entry: RouteEntry): Boolean =
    System.currentTimeMillis() - entry.createdAtMs > config.routeTtlMs

  private def decodeEnvelope(frames: Vector[Array[Byte]]): Either[TransportError, (PeerEnvelope, Array[Byte])] =
    frames match
      case Vector(delimiter, frame) if delimiter.isEmpty =>
        Right((PeerEnvelope.Req, frame))
      case Vector(frame) if frame.isEmpty =>
        Left(TransportError.ProtocolViolation("ZMQ request message has an empty wire frame"))
      case Vector(frame) =>
        Right((PeerEnvelope.Dealer, frame))
      case other if other.headOption.exists(_.isEmpty) =>
        Left(
          TransportError.ProtocolViolation(
            s"REQ peer message must contain an empty delimiter and exactly one wire frame, got ${other.size} frames"
          )
        )
      case other =>
        Left(
          TransportError.ProtocolViolation(
            s"DEALER peer message must contain exactly one wire frame, got ${other.size}"
          )
        )

  private def sendReply(
      sock: ZMQ.Socket,
      routingId: RoutingId,
      entry: RouteEntry,
      message: Message
  ): Either[TransportError, Unit] =
    if !sock.sendMore(entry.identity) then
      Left(TransportError.SendFailed("reply", s"failed to route reply to ${routingId.value}"))
    else
      entry.envelope match
        case PeerEnvelope.Req if !sock.sendMore(Array.emptyByteArray) =>
          Left(TransportError.SendFailed("reply", s"failed to send delimiter to ${routingId.value}"))
        case _ =>
          sendMessage(sock, message, "reply")

  private def sendMessage(sock: ZMQ.Socket, message: Message, operation: String): Either[TransportError, Unit] =
    val sent = sock.send(MessageCodec.encode(message), 0)
    if sent then Right(()) else Left(TransportError.SendFailed(operation, "failed to send message body"))

  private def decodeMessage(frame: Array[Byte]): Either[TransportError, Message] =
    MessageCodec.decode(frame, "request")

object ZmqTcpServer:
  def make[F[_]: Effect](config: ZmqTcpServerConfig): Resource[F, ServerTransport[F]] =
    val effect = Effect[F]
    Resource.make(effect.delay(new ZmqTcpServer[F](config)))(_.close)

  sealed private trait Command

  private object Command:
    final case class Reply(routingId: RoutingId, message: Message) extends Command

  private enum PeerEnvelope:
    case Req, Dealer

  final private case class RouteEntry(identity: Array[Byte], envelope: PeerEnvelope, createdAtMs: Long)
