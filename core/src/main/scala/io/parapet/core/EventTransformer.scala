package io.parapet.core

import io.parapet.Event

class EventTransformer(f: PartialFunction[Event, Event]) {
  def transform(e: Event): Event = if (f.isDefinedAt(e)) f(e) else e
}

object EventTransformer {

  val Noop: EventTransformer = EventTransformer(e => e)

  def apply(f: PartialFunction[Event, Event]): EventTransformer = {
    new EventTransformer(f)
  }
}