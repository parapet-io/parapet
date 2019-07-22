package io.parapet.example

import cats.effect.IO
import io.parapet.CatsApp
import io.parapet.core.Event.{Start, Stop}
import io.parapet.core.{Event, Process}
import io.parapet.implicits._

import scala.concurrent.duration._

object PingPongApp extends CatsApp {

  import PingProcess._
  import PongProcess._

  class PingProcess(pongProcess: Process[IO]) extends Process[IO] {

    import effectDsl._
    import flowDsl._

    override val name: String = "pingProcess"
    override val handle: Receive = {
      case Start => eval(println("")) ++ reply(sender => eval(println(s"$name received Start from: $sender")) ++ Ping ~> pongProcess)
      case Pong => reply(sender => eval(println(s"$name received Pong from: $sender")) ++ Ping ~> sender)
      case Stop => eval(println("PingProcess stopped"))
    }
  }

  object PingProcess {

    object Ping extends Event

  }

  class PongProcess extends Process[IO] {

    import effectDsl._
    import flowDsl._

    override val name: String = "pongProcess"
    override val handle: Receive = {
      case Ping => reply(sender => eval(println(s"$name received Ping from: $sender")) ++ delay(1.seconds) ++ Pong ~> sender)
      case Stop => eval(println("PongProcess stopped"))
    }
  }

  object PongProcess {

    object Pong extends Event

  }

  val pongProcess = new PongProcess()
  val pingProcess = new PingProcess(pongProcess)

  override def processes: IO[Seq[Process[IO]] ]= IO.pure(Seq(pingProcess, pongProcess))
  override val program: PingPongApp.Program = Start ~> pingProcess
}
