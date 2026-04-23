package io.parapet.core

import io.parapet.Event

/** Transforms events on their way to a specific receiver.
  *
  * Registered per [[io.parapet.ProcessRef]] via [[io.parapet.ParApp.eventTransformer]];
  * applied by the [[DslInterpreter]] just before an envelope is enqueued.
  *
  * Useful for cross-cutting concerns such as schema migration, encryption, or trace
  * propagation. Events outside `f`'s domain pass through unchanged.
  *
  * @param f partial function mapping input events to their transformed form.
  */
class EventTransformer(f: PartialFunction[Event, Event]) {
  /** Applies `f` to `e` if defined; otherwise returns `e` unchanged. */
  def transform(e: Event): Event = if (f.isDefinedAt(e)) f(e) else e
}

/** Constructors for [[EventTransformer]]. */
object EventTransformer {

  /** Identity transformer — returns every event unchanged. */
  val Noop: EventTransformer = EventTransformer(e => e)

  /** Builds a transformer from a partial function. */
  def apply(f: PartialFunction[Event, Event]): EventTransformer = {
    new EventTransformer(f)
  }
}
