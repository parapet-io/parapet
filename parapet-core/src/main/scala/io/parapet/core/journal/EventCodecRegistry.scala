package io.parapet.core.journal

import io.parapet.Event
import io.parapet.core.Events
import io.parapet.core.journal.EventCodec.Tag
import scala.collection.mutable

/** Resolves the [[EventCodec]].
  *
  * There is one codec per event type, bound to exactly one class and one tag.
  */
final class EventCodecRegistry private (
    private val byClass: Map[Class[?], EventCodec],
    private val byTag: Map[Tag, EventCodec]
):

  /** The codec for `event`, or `None` when its type is not registered - i.e. the event cannot be encoded. */
  def codecFor(event: Event): Option[EventCodec] = byClass.get(event.getClass)

  /** The codec that produced data under `tag`, or `None` when the tag is unknown - a decode of such data must fail loud
    * rather than proceed.
    */
  def codecForTag(tag: Tag): Option[EventCodec] = byTag.get(tag)

object EventCodecRegistry:

  private[core] val systemCodecs = Map[Class[?], EventCodec](
    classOf[Events.Registered] -> RegisteredEventCodec
  )

  /** Builds a registry, rejecting a class or a tag that is claimed more than once. */
  def apply(bindings: (Class[?], EventCodec)*): EventCodecRegistry =
    val builder = new Builder()
    systemCodecs.foreach(builder.add)
    bindings.foreach(builder.add)
    builder.build

  val empty: EventCodecRegistry = apply()

  private class Builder:
    val byClass = mutable.Map.empty[Class[?], EventCodec]
    val byTag   = mutable.Map.empty[Tag, EventCodec]

    def add(cls: Class[?], codec: EventCodec): Unit =
      require(!byClass.contains(cls), s"event ${cls.getName} already has a codec")
      require(!byTag.contains(codec.tag), s"tag '${codec.tag}' is claimed by more than one codec")
      byClass.put(cls, codec)
      byTag.put(codec.tag, codec)

    def build: EventCodecRegistry = new EventCodecRegistry(byClass.toMap, byTag.toMap)
