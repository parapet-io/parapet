package io.parapet.core

import cats.effect.IO
import io.parapet.core.Parapet.{CatsApp, Event, Process}

import io.parapet.core.catsInstances.flow._
import io.parapet.core.catsInstances.effect._
import scala.concurrent.duration._


object SendToSelfApp extends CatsApp {

  val recursiveProcess = new RecursiveProcess()
  import RecursiveProcess._

  override val processes: Array[Process[IO]] = Array(recursiveProcess)
  override val program: SendToSelfApp.ProcessFlow = RecEvent ~> recursiveProcess



  class RecursiveProcess extends Process[IO] {
    override val handle: Receive = {
      case RecEvent => delay(1.seconds) ++ eval(println("tick")) ++ RecEvent ~> selfRef
    }
  }

  object RecursiveProcess {

    object RecEvent extends Event

  }

}
