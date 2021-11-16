package io.parapet.core

import io.parapet.ProcessRef

import scala.collection.mutable

class EventTransformers(transformers: Map[ProcessRef, EventTransformer]) {

  def get(ref: ProcessRef): Option[EventTransformer] = {
    transformers.get(ref)
  }
}

object EventTransformers {
  class Builder {
    private val map = mutable.Map.empty[ProcessRef, EventTransformer]

    def add(ref: ProcessRef, t: EventTransformer): Builder = {
      map += ref -> t
      this
    }

    def build: EventTransformers = new EventTransformers(map.toMap)
  }

  def builder: Builder = new Builder

  def empty: EventTransformers = new EventTransformers(Map.empty)
}
