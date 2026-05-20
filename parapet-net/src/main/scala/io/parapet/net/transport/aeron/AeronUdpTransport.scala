package io.parapet.net.transport.aeron

import io.aeron.driver.MediaDriver
import io.aeron.logbuffer.FragmentHandler
import io.aeron.{Aeron, Publication}
import io.parapet.effect.Effect
import io.parapet.effect.Resource
import io.parapet.net.transport.{DatagramTransport, Message, ReceiveResult, TransportError}
import org.agrona.concurrent.UnsafeBuffer

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer

final case class AeronUdpConfig(
    channel: String,
    streamId: Int,
    embeddedMediaDriver: Boolean = false,
    directoryName: Option[String] = None,
    offerRetries: Int = 1000
)

final class AeronUdpTransport[F[_]] private (config: AeronUdpConfig)(using effect: Effect[F])
    extends DatagramTransport[F]:
  private val closed = new AtomicBoolean(false)

  private val embeddedDriver =
    if config.embeddedMediaDriver then
      val ctx = new MediaDriver.Context()
      config.directoryName.foreach(ctx.aeronDirectoryName)
      Some(MediaDriver.launchEmbedded(ctx))
    else None

  private val aeronContext =
    val context = new Aeron.Context()
    embeddedDriver.foreach(driver => context.aeronDirectoryName(driver.aeronDirectoryName()))
    config.directoryName.foreach(context.aeronDirectoryName)
    context

  private val aeron        = Aeron.connect(aeronContext)
  private val publication  = aeron.addPublication(config.channel, config.streamId)
  private val subscription = aeron.addSubscription(config.channel, config.streamId)

  def publish(message: Message): F[Either[TransportError, Unit]] =
    effect.blocking {
      if closed.get() then Left(TransportError.Closed("publish"))
      else
        message.parts.headOption match
          case None =>
            publishPart(Array.emptyByteArray)
          case Some(first) if message.parts.sizeIs == 1 =>
            publishPart(first)
          case Some(_) =>
            Left(TransportError.ProtocolViolation("Aeron datagrams support exactly one message part"))
    }

  private def publishPart(payload: Array[Byte]): Either[TransportError, Unit] =
    val data    = payload.clone()
    val buffer  = new UnsafeBuffer(data)
    var result  = publication.offer(buffer, 0, data.length)
    var retries = config.offerRetries

    while result < 0 && retries > 0 do
      Thread.`yield`()
      retries = retries - 1
      result = publication.offer(buffer, 0, data.length)

    if result < 0 then Left(TransportError.SendFailed("publish", s"Aeron publication result=$result"))
    else Right(())

  def receiveBatch(limit: Int): F[ReceiveResult[Vector[Message]]] =
    effect.blocking {
      if closed.get() then ReceiveResult.Failed(TransportError.Closed("receiveBatch"))
      else
        val messages = ListBuffer.empty[Message]
        val handler  = new FragmentHandler:
          override def onFragment(
              buffer: org.agrona.DirectBuffer,
              offset: Int,
              length: Int,
              header: io.aeron.logbuffer.Header
          ): Unit =
            val bytes = new Array[Byte](length)
            buffer.getBytes(offset, bytes)
            messages += Message.single(bytes)

        subscription.poll(handler, limit)
        if messages.isEmpty then ReceiveResult.Idle
        else ReceiveResult.Received(messages.toVector)
    }

  def close: F[Unit] =
    effect.delay {
      if closed.compareAndSet(false, true) then
        publication.close()
        subscription.close()
        aeron.close()
        embeddedDriver.foreach(_.close())
    }

object AeronUdpTransport:
  def make[F[_]: Effect](config: AeronUdpConfig): Resource[F, DatagramTransport[F]] =
    val effect = Effect[F]
    Resource.make(effect.delay(new AeronUdpTransport[F](config)))(_.close)
