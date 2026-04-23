package io.parapet.core.processes

import io.parapet.core.Events.SystemEvent
import io.parapet.core.Process
import io.parapet.core.processes.Sub._
import io.parapet.{Event, ProcessRef}

/** A multicast/filter process: re-publishes every non-system event it receives to a fixed
  * set of subscribers, optionally gated by a per-subscriber filter.
  *
  * Useful as the receiving end of a fan-out where multiple consumers need to observe the
  * same event stream without the producers knowing about them.
  *
  * @param subs subscribers to forward events to.
  */
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

/** Constructors and types for [[Sub]]. */
object Sub {
  /** A single subscription.
    *
    * @param ref    process to forward matching events to.
    * @param filter partial function selecting which events qualify; defaults to "match all".
    */
  case class Subscription(ref: ProcessRef, filter: PartialFunction[Event, Unit] = {
    case _ => ()
  })

  /** Builds a [[Sub]] from a sequence of [[Subscription]]s. */
  def apply[F[_]](subs: Seq[Subscription]): Sub[F] = new Sub[F](subs)
}
