package io.parapet.core.processes

import io.parapet.core.Events.SystemEvent
import io.parapet.core.Process
import io.parapet.core.processes.Sub._
import io.parapet.{Event, ProcessRef}

class Sub[F[_]](subs: Seq[Subscription]) extends Process[F] {

  import dsl._

  override def handle: Receive = {
    case _: SystemEvent => unit
    case e: Event =>
      subs.map { sub =>
        if (sub.filter.isDefinedAt(e)) {
          e ~> sub.ref
        } else {
          unit
        }
      }.fold(unit)(_ ++ _)
  }
}

object Sub {
  case class Subscription(ref: ProcessRef, filter: PartialFunction[Event, Unit] = {
    case _ => ()
  })

  def apply[F[_]](subs: Seq[Subscription]): Sub[F] = new Sub[F](subs)
}
