package io.parapet.net.transport.zmq

import io.parapet.effect.Effect
import io.parapet.effect.Resource
import io.parapet.net.{Endpoint, TransportProtocol}
import io.parapet.net.transport.{DuplexTransport, Message, MessageCodec, ReceiveResult, TransportError}
import org.zeromq.{SocketType, ZContext, ZMQ}

import scala.collection.mutable.ListBuffer

final case class ZmqTcpDuplexConfig(
    remote: Endpoint,
    receiveTimeoutMs: Int = 250,
    workerPollMs: Int = 20,
    ioThreads: Int = 1,
    inboundCapacity: Int = 1024
):
  require(remote.protocol == TransportProtocol.Tcp, "ZMQ TCP duplex transport requires a tcp endpoint")
  require(workerPollMs > 0, "workerPollMs must be positive")
  require(inboundCapacity > 0, "inboundCapacity must be positive")

/** DEALER-backed duplex transport.
  *
  * The DEALER socket is owned by a single [[ZmqSocketWorker]]: `receive` drains polled messages and `send` submits an
  * explicit send command to that worker. This replaces the earlier coarse `socketLock`, which was safe but coupled send
  * latency to the full receive poll window.
  */
final class ZmqTcpDuplexTransport[F[_]](config: ZmqTcpDuplexConfig)(using effect: Effect[F]) extends DuplexTransport[F]:
  import ZmqTcpDuplexTransport.Command

  private val context = new ZContext(config.ioThreads)
  private val socket  = context.createSocket(SocketType.DEALER)

  socket.setReceiveTimeOut(config.workerPollMs)
  socket.setLinger(0)
  socket.connect(config.remote.uri)

  private val loop =
    new ZmqSocketWorker[Command, Message](
      context,
      socket,
      readInbound,
      handleCommand,
      s"zmq-duplex-${config.remote.port}",
      config.inboundCapacity
    )

  def send(message: Message): F[Either[TransportError, Unit]] =
    effect.blocking(loop.submit(Command.Send(message)))

  def receive: F[ReceiveResult[Message]] =
    effect.blocking(loop.poll(config.receiveTimeoutMs))

  def close: F[Unit] =
    effect.delay(loop.close())

  private def handleCommand(sock: ZMQ.Socket, command: Command): Either[TransportError, Unit] =
    command match
      case Command.Send(message) => sendMessage(sock, message, "send")

  private def readInbound(sock: ZMQ.Socket): ReceiveResult[Message] =
    Option(sock.recv(0)) match
      case None =>
        ReceiveResult.Idle
      case Some(first) =>
        val parts = ListBuffer(first)
        while sock.hasReceiveMore do parts += sock.recv(0)
        decodeMessage(parts.toVector) match
          case Right(message) => ReceiveResult.Received(message)
          case Left(error)    => ReceiveResult.Failed(error)

  private def sendMessage(sock: ZMQ.Socket, message: Message, operation: String): Either[TransportError, Unit] =
    val sent = sock.send(MessageCodec.encode(message), 0)
    if sent then Right(())
    else Left(TransportError.SendFailed(operation, s"failed to send to ${config.remote.uri}"))

  private def decodeMessage(frames: Vector[Array[Byte]]): Either[TransportError, Message] =
    frames match
      case Vector(frame) => MessageCodec.decode(frame, "duplex")
      case other         =>
        Left(
          TransportError.ProtocolViolation(s"ZMQ duplex message must contain exactly one wire frame, got ${other.size}")
        )

object ZmqTcpDuplexTransport:
  def make[F[_]: Effect](config: ZmqTcpDuplexConfig): Resource[F, DuplexTransport[F]] =
    val effect = Effect[F]
    Resource.make(effect.delay(new ZmqTcpDuplexTransport[F](config)))(_.close)

  sealed private trait Command

  private object Command:
    final case class Send(message: Message) extends Command
