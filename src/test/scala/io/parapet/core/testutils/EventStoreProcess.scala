package io.parapet.core.testutils

import cats.effect.IO
import io.parapet.core.Parapet._
import io.parapet.core.catsInstances.effect._
import io.parapet.core.catsInstances.flow._

import scala.collection.mutable.ListBuffer

class EventStoreProcess extends Process[IO] {
  override val name: String = "π-event-store-process"
  private val _events = ListBuffer[Event]()
  override val handle: Receive = {
    case Start | Stop => empty
    case e            => eval(_events += e)
  }

  def events: Seq[Event] = collection.immutable.Seq(_events: _*)

}
