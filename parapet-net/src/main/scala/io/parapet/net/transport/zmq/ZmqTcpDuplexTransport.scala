package io.parapet.net.transport.zmq

import io.parapet.effect.Effect
import io.parapet.effect.Resource
import io.parapet.net.{Endpoint, TransportProtocol}
import io.parapet.net.transport.{DuplexTransport, Message, MessageCodec, ReceiveResult, TransportError}
import org.zeromq.{SocketType, ZContext, ZMQException}

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer

final case class ZmqTcpDuplexConfig(
    remote: Endpoint,
    receiveTimeoutMs: Int = 250,
    ioThreads: Int = 1
):
  require(remote.protocol == TransportProtocol.Tcp, "ZMQ TCP duplex transport requires a tcp endpoint")

final class ZmqTcpDuplexTransport[F[_]] private (config: ZmqTcpDuplexConfig)(using effect: Effect[F])
    extends DuplexTransport[F]:
  private val context    = new ZContext(config.ioThreads)
  private val socket     = context.createSocket(SocketType.DEALER)
  private val closed     = new AtomicBoolean(false)
  private val socketLock = new Object

  socket.setReceiveTimeOut(config.receiveTimeoutMs)
  socket.setLinger(0)
  socket.connect(config.remote.uri)

  def send(message: Message): F[Either[TransportError, Unit]] =
    effect.blocking {
      if closed.get() then Left(TransportError.Closed("send"))
      else
        socketLock.synchronized {
          try sendMessage(message, "send")
          catch case error: ZMQException => Left(TransportError.Unexpected(error))
        }
    }

  def receive: F[ReceiveResult[Message]] =
    effect.blocking {
      if closed.get() then ReceiveResult.Failed(TransportError.Closed("receive"))
      else
        socketLock.synchronized {
          try receiveOnce()
          catch
            case _: ZMQException if closed.get() =>
              ReceiveResult.Failed(TransportError.Closed("receive"))
            case error: ZMQException =>
              ReceiveResult.Failed(TransportError.Unexpected(error))
        }
    }

  private def receiveOnce(): ReceiveResult[Message] =
    Option(socket.recv(0)) match
      case None =>
        ReceiveResult.Idle
      case Some(first) =>
        val parts = ListBuffer(first)
        while socket.hasReceiveMore do parts += socket.recv(0)
        decodeMessage(parts.toVector) match
          case Right(message) => ReceiveResult.Received(message)
          case Left(error)    => ReceiveResult.Failed(error)

  private def sendMessage(message: Message, operation: String): Either[TransportError, Unit] =
    val sent = socket.send(MessageCodec.encode(message), 0)

    if sent then Right(())
    else Left(TransportError.SendFailed(operation, s"failed to send to ${config.remote.uri}"))

  private def decodeMessage(frames: Vector[Array[Byte]]): Either[TransportError, Message] =
    frames match
      case Vector(frame) => MessageCodec.decode(frame, "duplex")
      case other         =>
        Left(
          TransportError.ProtocolViolation(s"ZMQ duplex message must contain exactly one wire frame, got ${other.size}")
        )

  def close: F[Unit] =
    effect.delay {
      if closed.compareAndSet(false, true) then
        socketLock.synchronized {
          context.close()
        }
    }

object ZmqTcpDuplexTransport:
  def make[F[_]: Effect](config: ZmqTcpDuplexConfig): Resource[F, DuplexTransport[F]] =
    val effect = Effect[F]
    Resource.make(effect.delay(new ZmqTcpDuplexTransport[F](config)))(_.close)
