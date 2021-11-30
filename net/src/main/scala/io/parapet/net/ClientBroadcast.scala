package io.parapet.net

import io.parapet.core.Dsl.DslF
import io.parapet.core.Process
import io.parapet.core.api.Cmd.netClient
import io.parapet.{Event, ProcessRef}

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

class ClientBroadcast[F[_]](refs: Seq[ProcessRef],
                            reply: ProcessRef,
                            acksRequired: Int,
                            timeout: FiniteDuration) extends Process[F] {

  import dsl._

  private val _replies = mutable.ListBuffer.empty[netClient.Rep]
  private var _event: Event = _
  private var _started = false
  private var _completed = false

  def complete: DslF[F, Unit] = flow {
    if (!_completed && _replies.size >= acksRequired) {
      eval {
        _completed = true
      } ++ ClientBroadcast.Done(_replies.toList) ~> reply
    } else unit
  }

  override def handle: Receive = {
    case rep: netClient.Rep =>
      if (rep.data != null) {
        eval {
          _replies += rep
        } ++ complete
      } else unit

    case ClientBroadcast.Send(data) =>
      eval {
        if (_started) {
          throw new IllegalStateException("broadcast is in progress")
        }
        _started = true
        _event = netClient.Send(data, Option(ref))
      } ++ execute
    case ClientBroadcast.Timeout => execute
  }

  private def execute: DslF[F, Unit] = {
    eval(_replies.clear()) ++
      par(refs.map(ref => _event ~> ref): _*) ++ fork(delay(timeout) ++ ClientBroadcast.Timeout ~> ref)
  }
}

object ClientBroadcast {
  object Timeout extends Event

  case class Send(data: Array[Byte]) extends Event

  case class Done(replies: List[netClient.Rep]) extends Event
}
