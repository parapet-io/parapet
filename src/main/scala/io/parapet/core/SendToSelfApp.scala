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
  override val program: SendToSelfApp.ProcessFlow = Counter(10000000) ~> recursiveProcess



  class RecursiveProcess extends Process[IO] {
    override val handle: Receive = {
      case Counter(i) =>
        if (i == 0) terminate
        else eval(println(i)) ++ Counter(i - 1) ~> selfRef
    }
  }

  object RecursiveProcess {

    case class Counter(value: Int) extends Event

  }

}
