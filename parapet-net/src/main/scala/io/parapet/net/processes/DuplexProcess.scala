package io.parapet.net.processes

import com.typesafe.scalalogging.Logger
import io.parapet.core.Events.{Start, Stop}
import io.parapet.core.Process
import io.parapet.net.processes.DuplexProcess.*
import io.parapet.net.transport.{DuplexTransport, Message, ReceiveResult, TransportError}
import io.parapet.{Event, ProcessRef}
import org.slf4j.LoggerFactory

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class DuplexProcess[F[_]](
    transport: DuplexTransport[F],
    pollDelay: FiniteDuration = 10.millis,
    override val ref: ProcessRef[Request] = ProcessRef[Request]("net-duplex")
) extends Process[F, Request, Response | Failed]:

  import dsl.*

  private val logger   = Logger(LoggerFactory.getLogger(getClass.getCanonicalName))
  private val requests = new ConcurrentHashMap[String, ProcessRef[Response | Failed]]()

  private def receiveLoop: Program =
    suspend(transport.receive).flatMap {
      case ReceiveResult.Received(message) =>
        routeReceived(message)

      case ReceiveResult.Idle =>
        unit

      case ReceiveResult.Failed(error: TransportError.Closed) =>
        failPending(error)

      case ReceiveResult.Failed(error) =>
        warn(s"receive error: $error")
    } ++ delay(pollDelay) ++ receiveLoop

  override def handle: Receive = {
    case Start =>
      fork(receiveLoop).void

    case Request(data) =>
      dsl.unsafe.withSender { sender =>
        val id = UUID.randomUUID().toString
        eval {
          requests.put(id, sender)
        } ++ suspend(transport.send(Message(id, data))).flatMap {
          case Right(_) =>
            unit
          case Left(error) =>
            eval {
              logger.error("failed to send a request. sender: {}", sender)
              requests.remove(id)
            } ++ (Failed(error) ~> sender)
        }
      }

    case Stop =>
      suspend(transport.close)
  }

  private def routeReceived(message: Message): Program =
    val id = message.correlationId
    Option(requests.remove(id)) match
      case Some(sender) =>
        Response(message.payload) ~> sender
      case None =>
        warn("dropping response with unknown correlation id: {}", id)

  private def failPending(error: TransportError): Program =
    val senders = requests.values().asScala.toVector
    eval {
      requests.clear()
    } ++ senders.foldLeft(unit) { (acc, sender) =>
      acc ++ (Failed(error) ~> sender)
    }

  private def warn(message: => String): Program =
    eval(logger.warn(message))

  private def warn(message: String, arg: Any): Program =
    eval(logger.warn(message, arg))

object DuplexProcess:
  final case class Request(data: Array[Byte])    extends Event
  final case class Response(data: Array[Byte])   extends Event
  final case class Failed(error: TransportError) extends Event
