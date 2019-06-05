package io.parapet.core

import cats.effect.IO
import io.parapet.core.Parapet.CatsApp
import io.parapet.core.Parapet.Process
import io.parapet.core.Event._
import io.parapet.core.catsInstances.flow._
import io.parapet.core.catsInstances.effect._
import scala.concurrent.duration._
import io.parapet.implicits._

object PingPongApp extends CatsApp {

  import PingProcess._
  import PongProcess._

  class PingProcess(pongProcess: Process[IO]) extends Process[IO] {
    override val name: String = "pingProcess"
    override val handle: Receive = {
      case Start => reply(sender => eval(println(s"$name received Start from: ${sender.ref}")) ++ Ping ~> pongProcess)
      case Pong => reply(sender => eval(println(s"$name received Pong from: ${sender.ref}")) ++ Ping ~> sender)
      case Stop => eval(println("PingProcess stopped"))
    }
  }

  object PingProcess {
    object Ping extends Event
  }

  class PongProcess extends Process[IO] {
    override val name: String = "pongProcess"
    override val handle: Receive = {
      case Ping => reply(sender => eval(println(s"$name received Ping from: ${sender.ref}")) ++ delay(1.seconds) ++ Pong ~> sender)
      case Stop => eval(println("PongProcess stopped"))
    }
  }

  object PongProcess {

    object Pong extends Event

  }

  val pongProcess = new PongProcess()
  val pingProcess = new PingProcess(pongProcess)

  override val processes: Array[Parapet.Process[IO]] = Array(pingProcess, pongProcess)
  override val program: PingPongApp.ProcessFlow = Start ~> pingProcess
}


