package io.parapet.net

import cats.effect.Concurrent
import cats.implicits._
import io.parapet.core.Dsl.DslF
import io.parapet.core.api.Cmd.netClient
import io.parapet.core.{Channel, Process}
import io.parapet.net.ClientBroadcast._
import io.parapet.{Event, ProcessRef}
import io.parapet.net.Exceptions._

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Try

class ClientBroadcast[F[_] : Concurrent] extends Process[F] {

  import dsl._

  val counter = new AtomicInteger()

  override def handle: Receive = {
    case Send(e, receivers, timeout) =>
      withSender { sender =>
        val channels = List.fill(receivers.size)(Channel[F])
        register(ref, channels: _*) ++
          par(receivers.zip(channels).map {
            case (receiver, chan) => send(e, chan, receiver, timeout)
          }: _*).flatMap { fibers =>
            fibers.map(_.join).sequence
          }.flatMap(res => Response(res) ~> sender) ++
          channels.map(ch => halt(ch.ref)).fold(unit)(_ ++ _)
      }

  }

  private def send(data: Array[Byte], channel: Channel[F],
                   receiver: ProcessRef, timeout: FiniteDuration): DslF[F, Try[Event]] = flow {
    for {
      res <- if (timeout != Duration.Zero) {
        withTimeout(channel.send(netClient.Send(data, Option(channel.ref)), receiver), timeout)
      } else {
        channel.send(netClient.Send(data, Option(channel.ref)), receiver)
      }
    } yield res
  }

  private def withTimeout(f: => DslF[F, Try[Event]],
                          timeout: FiniteDuration): DslF[F, Try[Event]] = {
    race(f,
      delay(timeout) ++ eval(scala.util.Failure[Event](TimeoutException("request timed out")))
    ).map {
      case scala.util.Left(v) => v
      case scala.util.Right(v) => v
    }
  }
}

object ClientBroadcast {

  // ClientBroadcast[F[_] : Concurrent]
  def apply[F[_] : Concurrent] = new ClientBroadcast[F]

  case class Send(data: Array[Byte], receivers: List[ProcessRef],
                  timeout: FiniteDuration = Duration.Zero) extends Event

  case class Response(res: Seq[Try[Event]]) extends Event
}