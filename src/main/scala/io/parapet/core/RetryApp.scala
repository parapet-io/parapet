package io.parapet.core

import Parapet._
import cats.effect.IO

import io.parapet.core.catsInstances.effect._
import io.parapet.core.catsInstances.flow._

object RetryApp extends CatsApp {

  object EmptyEvent extends Event

  override def program: RetryApp.ProcessFlow = {
    val p = Process[IO] {
      case _ => eval(throw new RuntimeException)
    }

    EmptyEvent ~> p

  }
}
