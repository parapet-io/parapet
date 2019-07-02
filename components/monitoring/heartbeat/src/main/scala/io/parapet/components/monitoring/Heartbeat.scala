package io.parapet.components.monitoring

import io.parapet.components.monitoring.Heartbeat._
import io.parapet.core.{Event, Process, ProcessRef}
import io.parapet.implicits._

import scala.concurrent.duration.{FiniteDuration, _}

class Heartbeat[F[_]](initialTimeout: FiniteDuration) extends Process[F] {

  import effectDsl._
  import flowDsl._

  private var timestamp = Duration(System.nanoTime, NANOSECONDS)
  private var timeout = initialTimeout
  private var client = ProcessRef.UndefinedRef
  private var receiver = ProcessRef.UndefinedRef


  val uninitialized: Receive = {
    case Init(c, r) => eval {
      client = c
      receiver = r
    } ++ eval(println("initialized")) ++ switch(ready)
  }

  val ready: Receive = {
    case Run =>
      eval {
        timestamp = System.nanoTime.nanos
      } ++
        use(timestamp)(ts =>
          Ping(ts.length) ~> receiver ++
            await(classOf[Ack], Timeout(ts.length) ~> selfRef, timeout)
        )
    case Ack(ts) =>
      eval(println("received ack: " + ts)) ++
        eval(timeout = timeout.max(System.nanoTime.nanos - ts.nanos)) ++
        flow {
          if (ts == timestamp.length) {
            eval(timestamp = System.nanoTime.nanos) ++ Run ~> selfRef
          } else eval(println("timeout event sent to the client")) // timeout event sent to the client
        }
    case t@Timeout(ts) =>
      eval(println("Timeout")) ++
        flow {
          if (ts == timestamp.length) {
            eval(timestamp = System.nanoTime.nanos) ++ t ~> client
          } else empty // ack was delivered with delay
        }
  }


  override def handle: Receive = {
    uninitialized
  }

}

object Heartbeat {

  def apply[F[_]](initialTimeout: FiniteDuration): Process[F] = new Heartbeat(initialTimeout)

  case class Init(client: ProcessRef, receiver: ProcessRef) extends Event

  object Run extends Event

  case class Ping(timestamp: Long) extends Event

  case class Ack(timestamp: Long) extends Event

  case class Timeout(timestamp: Long) extends Event

}