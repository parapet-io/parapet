package io.parapet.net.transport.zmq

import io.parapet.effect.Effect
import io.parapet.effect.Resource
import io.parapet.net.{Endpoint, TransportProtocol}
import io.parapet.net.transport.*
import org.zeromq.{SocketType, ZContext, ZMQ}

import java.util.UUID
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

final case class ZmqTcpServerConfig(
    bind: Endpoint,
    receiveTimeoutMs: Int = 250,
    workerPollMs: Int = 20,
    ioThreads: Int = 1,
    routeTtlMs: Long = 30_000,
    inboundCapacity: Int = 1024
):
  require(bind.protocol == TransportProtocol.Tcp, "ZMQ TCP server requires a tcp endpoint")
  require(workerPollMs > 0, "workerPollMs must be positive")
  require(routeTtlMs > 0, "routeTtlMs must be positive")
  require(inboundCapacity > 0, "inboundCapacity must be positive")

/** ROUTER-backed request/reply server transport.
  *
  * The ROUTER socket is owned by a single [[ZmqSocketWorker]]: `receive` drains messages the worker has polled, and
  * `reply` submits an explicit reply command to that worker. Routing ids are opaque, server-local handles; ZMQ identity
  * bytes stay inside the transport.
  */
final class ZmqTcpServer[F[_]] private (config: ZmqTcpServerConfig)(using effect: Effect[F]) extends ServerTransport[F]:
  import ZmqTcpServer.{Command, RouteEntry}

  private val context = new ZContext(config.ioThreads)
  private val socket  = context.createSocket(SocketType.ROUTER)

  private val routes               = mutable.HashMap.empty[String, RouteEntry]
  private val routeSweepIntervalMs = math.max(1L, config.routeTtlMs / 10)
  private var lastSweepMs          = 0L

  socket.setReceiveTimeOut(config.workerPollMs)
  socket.setLinger(0)
  socket.bind(config.bind.uri)

  private val loop =
    new ZmqSocketWorker[Command, RoutedMessage](
      context,
      socket,
      readInbound,
      handleCommand,
      s"zmq-server-${config.bind.port}",
      config.inboundCapacity
    )

  def receive: F[ReceiveResult[RoutedMessage]] =
    effect.blocking(loop.poll(config.receiveTimeoutMs))

  def reply(routingId: RoutingId, message: Message): F[Either[TransportError, Unit]] =
    effect.blocking(loop.submit(Command.Reply(routingId, message)))

  def close: F[Unit] =
    effect.delay(loop.close())

  private def handleCommand(sock: ZMQ.Socket, command: Command): Either[TransportError, Unit] =
    command match
      case Command.Reply(routingId, message) =>
        takeRoute(routingId) match
          case None        => Left(TransportError.UnknownRoute(routingId))
          case Some(entry) => sendReply(sock, routingId, entry, message)

  private def readInbound(sock: ZMQ.Socket): ReceiveResult[RoutedMessage] =
    expireRoutes()
    val identity = sock.recv(0)
    if identity == null then ReceiveResult.Idle
    else
      val frames = ListBuffer.empty[Array[Byte]]
      while sock.hasReceiveMore do frames += sock.recv(0)

      decodeEnvelope(frames.toVector) match
        case Left(error) =>
          ReceiveResult.Failed(error)
        case Right((envelope, message)) =>
          val routingId = registerRoute(identity, envelope)
          ReceiveResult.Received(RoutedMessage(routingId, message))

  private def registerRoute(identity: Array[Byte], socketType: SocketType): RoutingId =
    val routingId = RoutingId(UUID.randomUUID().toString)
    routes.update(routingId.value, RouteEntry(identity.clone(), socketType, System.currentTimeMillis()))
    routingId

  private def takeRoute(routingId: RoutingId): Option[RouteEntry] =
    routes.remove(routingId.value).filterNot(entry => isExpired(entry, System.currentTimeMillis()))

  private def expireRoutes(): Unit =
    val now = System.currentTimeMillis()
    if now - lastSweepMs >= routeSweepIntervalMs then
      lastSweepMs = now
      routes.filterInPlace((_, entry) => !isExpired(entry, now))

  private def isExpired(entry: RouteEntry, now: Long): Boolean =
    now - entry.createdAtMs > config.routeTtlMs

  private def violation(message: String): Either[TransportError, Nothing] =
    Left(TransportError.ProtocolViolation(message))

  private def decodeEnvelope(frames: Vector[Array[Byte]]): Either[TransportError, (SocketType, Message)] =
    def decode(socketType: SocketType, frame: Array[Byte]): Either[TransportError, (SocketType, Message)] =
      if frame.isEmpty then violation(s"$socketType peer message has an empty wire frame")
      else decodeMessage(frame).map(message => (socketType, message))

    frames.size match
      case 0 =>
        violation("ZMQ request message must contain one or two frames, got 0")

      case 1 =>
        decode(SocketType.DEALER, frames.head)

      case 2 =>
        val delimiter = frames.head
        val frame     = frames.last

        if delimiter.nonEmpty then violation("REQ peer message is missing empty delimiter frame")
        else decode(SocketType.REQ, frame)

      case n =>
        violation(s"ZMQ request message must contain one or two frames, got $n")

  private def sendReply(
      sock: ZMQ.Socket,
      routingId: RoutingId,
      entry: RouteEntry,
      message: Message
  ): Either[TransportError, Unit] =
    if !sock.sendMore(entry.identity) then
      Left(TransportError.SendFailed("reply", s"failed to route reply to ${routingId.value}"))
    else
      entry.socketType match
        case SocketType.REQ if !sock.sendMore(Array.emptyByteArray) =>
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

  final private case class RouteEntry(identity: Array[Byte], socketType: SocketType, createdAtMs: Long)
