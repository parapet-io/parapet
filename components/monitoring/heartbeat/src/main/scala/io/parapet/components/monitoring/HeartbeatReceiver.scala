package io.parapet.components.monitoring

import io.parapet.core.Process
import io.parapet.implicits._
import io.parapet.components.monitoring.Heartbeat._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

class HeartbeatReceiver[F[_]](time: FiniteDuration = 0.millis) extends Process[F] {

  import flowDsl._
  import effectDsl._

  override def handle: Receive = {
    case Ping(ts) =>
      eval(println(s"HeartbeatReceiver received Ping($ts)")) ++
        delay(time) ++
        reply(sender => Ack(ts) ~> sender)
  }
}

object HeartbeatReceiver {
  def apply[F[_]](time: FiniteDuration = 0.millis): Process[F] = new HeartbeatReceiver(time)
}