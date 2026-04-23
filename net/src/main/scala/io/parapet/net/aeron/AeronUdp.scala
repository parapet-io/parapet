package io.parapet.net.aeron

import io.aeron.driver.MediaDriver
import io.aeron.logbuffer.FragmentHandler
import io.aeron.{Aeron, Publication}
import io.parapet.effect.Effect
import io.parapet.net.DatagramTransport
import org.agrona.concurrent.UnsafeBuffer

import scala.collection.mutable.ListBuffer

/** Connection settings for [[AeronUdpTransport]].
  *
  * @param channel             Aeron channel URI (e.g. `aeron:udp?endpoint=localhost:40123`).
  * @param streamId            Aeron stream id used for both publication and subscription.
  * @param embeddedMediaDriver if `true`, launches an in-process [[MediaDriver]] —
  *                            convenient for tests and standalone deployments.
  * @param directoryName       optional override for the Aeron CnC directory.
  * @param offerRetries        number of busy-spin retries on a failed `offer` before
  *                            giving up.
  */
final case class AeronUdpConfig(
    channel: String,
    streamId: Int,
    embeddedMediaDriver: Boolean = false,
    directoryName: Option[String] = None,
    offerRetries: Int = 1000
)

/** Aeron-backed [[DatagramTransport]].
  *
  * Wraps an Aeron [[Publication]]/[[io.aeron.Subscription]] pair so the broader parapet
  * stack can speak to peers via UDP without depending on Aeron types directly.
  *
  * Lifecycle: the transport opens its publication, subscription, and (optionally) an
  * embedded [[MediaDriver]] eagerly; [[close]] tears them down in reverse order.
  */
final class AeronUdpTransport[F[_]](config: AeronUdpConfig)(using effect: Effect[F]) extends DatagramTransport[F]:
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

  private val aeron = Aeron.connect(aeronContext)
  private val publication = aeron.addPublication(config.channel, config.streamId)
  private val subscription = aeron.addSubscription(config.channel, config.streamId)

  def publish(payload: Array[Byte]): F[Unit] =
    effect.blocking {
      val data = payload.clone()
      val buffer = new UnsafeBuffer(data)
      var result = publication.offer(buffer, 0, data.length)
      var retries = config.offerRetries

      while result < 0 && retries > 0 do
        Thread.`yield`()
        retries = retries - 1
        result = publication.offer(buffer, 0, data.length)

      if result < 0 then
        throw new IllegalStateException(s"failed to publish Aeron message, publication result=$result")
      ()
    }

  def receiveBatch(limit: Int): F[List[Array[Byte]]] =
    effect.blocking {
      val messages = ListBuffer.empty[Array[Byte]]
      val handler = new FragmentHandler:
        override def onFragment(buffer: org.agrona.DirectBuffer, offset: Int, length: Int, header: io.aeron.logbuffer.Header): Unit =
          val bytes = new Array[Byte](length)
          buffer.getBytes(offset, bytes)
          messages += bytes

      subscription.poll(handler, limit)
      messages.toList
    }

  def close: F[Unit] =
    effect.delay {
      publication.close()
      subscription.close()
      aeron.close()
      embeddedDriver.foreach(_.close())
      ()
    }
