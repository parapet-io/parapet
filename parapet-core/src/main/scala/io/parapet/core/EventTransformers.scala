package io.parapet.core

import io.parapet.ProcessRef

import scala.collection.mutable

/** Immutable registry of [[EventTransformer]]s keyed by [[ProcessRef]].
  *
  * Looked up by [[DslInterpreter]] for every send so that transformers can be applied to the targeted process's
  * incoming events.
  *
  * @param transformers
  *   map from receiver ref to its transformer.
  */
class EventTransformers(transformers: Map[ProcessRef[?], EventTransformer]) {

  /** Returns the transformer registered for `ref`, if any. */
  def get(ref: ProcessRef[?]): Option[EventTransformer] =
    transformers.get(ref)
}

/** Builder helpers for [[EventTransformers]]. */
object EventTransformers {

  /** Mutable builder collecting per-process transformer registrations.
    *
    * Used internally by [[io.parapet.ParApp]] - applications register transformers via
    * [[io.parapet.ParApp.eventTransformer]] before the runtime starts.
    */
  class Builder {
    private val map = mutable.Map.empty[ProcessRef[?], EventTransformer]

    /** Registers `t` for `ref`. Replaces any previous transformer for the same ref. */
    def add(ref: ProcessRef[?], t: EventTransformer): Builder = {
      map += ref -> t
      this
    }

    /** Snapshots the registrations into an immutable [[EventTransformers]]. */
    def build: EventTransformers = new EventTransformers(map.toMap)
  }

  /** Creates a fresh empty [[Builder]]. */
  def builder: Builder = new Builder

  /** Returns an [[EventTransformers]] with no registrations. */
  def empty: EventTransformers = new EventTransformers(Map.empty)
}
