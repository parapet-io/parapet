package io.parapet.tests.intg.scheduler

import io.parapet.Event

/** Event used by scheduler tests.
  *
  * @param submitterId
  *   logical id of the fiber that enqueued this event.
  * @param seqNumber
  *   globally unique across a run (across all submitters and receivers). This makes every event uniquely identifiable.
  */
final case class TestEvent(submitterId: Int, seqNumber: Int) extends Event

object TestEvent {

  /** Narrowing cast used in a few places where callers already know an [[Event]] is a [[TestEvent]].
    */
  def cast(e: Event): TestEvent = e.asInstanceOf[TestEvent]

}
