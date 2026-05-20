package io.parapet.net.transport.zmq

import io.parapet.effect.Effect
import io.parapet.effect.Resource
import io.parapet.net.{Endpoint, TransportProtocol}
import io.parapet.net.transport.{ClientTransport, Message, TransportError}
import org.zeromq.{SocketType, ZContext, ZMQException}

import java.util.concurrent.atomic.AtomicBoolean

final case class ZmqTcpClientConfig(
    remote: Endpoint,
    receiveTimeoutMs: Int = 5000,
    ioThreads: Int = 1
):
  require(remote.protocol == TransportProtocol.Tcp, "ZMQ TCP client requires a tcp endpoint")

final class ZmqTcpClient[F[_]] private (config: ZmqTcpClientConfig)(using effect: Effect[F]) extends ClientTransport[F]:
  private val context = new ZContext(config.ioThreads)
  private val socket  = context.createSocket(SocketType.REQ)
  private val closed  = new AtomicBoolean(false)

  socket.setReceiveTimeOut(config.receiveTimeoutMs)
  socket.setLinger(0)
  socket.connect(config.remote.uri)

  def request(message: Message): F[Either[TransportError, Message]] =
    effect.blocking {
      if closed.get() then Left(TransportError.Closed("request"))
      else
        try
          sendMessage(message).flatMap { _ =>
            Option(socket.recv(0)) match
              case None =>
                Left(TransportError.TimedOut("request"))
              case Some(first) =>
                val parts = Vector.newBuilder[Array[Byte]]
                parts += first
                while socket.hasReceiveMore do parts += socket.recv(0)
                Right(Message(parts.result()))
          }
        catch
          case error: ZMQException =>
            Left(TransportError.Unexpected(error))
    }

  private def sendMessage(message: Message): Either[TransportError, Unit] =
    val parts = if message.parts.isEmpty then Vector(Array.emptyByteArray) else message.parts
    var sent  = true
    parts.zipWithIndex.foreach { case (part, index) =>
      val last = index == parts.size - 1
      sent = sent && (if last then socket.send(part, 0) else socket.sendMore(part))
    }
    if sent then Right(()) else Left(TransportError.SendFailed("request", s"failed to send to ${config.remote.uri}"))

  def close: F[Unit] =
    effect.delay {
      if closed.compareAndSet(false, true) then context.close()
    }

object ZmqTcpClient:
  def make[F[_]: Effect](config: ZmqTcpClientConfig): Resource[F, ClientTransport[F]] =
    val effect = Effect[F]
    Resource.make(effect.delay(new ZmqTcpClient[F](config)))(_.close)
