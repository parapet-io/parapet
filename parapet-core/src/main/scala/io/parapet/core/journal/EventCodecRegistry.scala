package io.parapet.core.journal

import io.parapet.Event
import io.parapet.core.journal.EventCodec.Tag

/** Resolves the [[EventCodec]].
  *
  * There is one codec per event type, bound to exactly one class and one tag.
  */
final class EventCodecRegistry private (byClass: Map[Class[?], EventCodec], byTag: Map[Tag, EventCodec]):

  /** The codec for `event`, or `None` when its type is not registered - i.e. the event cannot be encoded. */
  def codecFor(event: Event): Option[EventCodec] = byClass.get(event.getClass)

  /** The codec that produced data under `tag`, or `None` when the tag is unknown - a decode of such data must fail loud
    * rather than proceed.
    */
  def codecForTag(tag: Tag): Option[EventCodec] = byTag.get(tag)

object EventCodecRegistry:

  /** Builds a registry, rejecting a class or a tag that is claimed more than once. */
  def apply(bindings: (Class[?], EventCodec)*): EventCodecRegistry =
    val byClass = bindings.foldLeft(Map.empty[Class[?], EventCodec]) { case (acc, (cls, codec)) =>
      require(!acc.contains(cls), s"event ${cls.getName} already has a codec")
      acc.updated(cls, codec)
    }
    val byTag = bindings.foldLeft(Map.empty[Tag, EventCodec]) { case (acc, (_, codec)) =>
      require(!acc.contains(codec.tag), s"tag '${codec.tag}' is claimed by more than one codec")
      acc.updated(codec.tag, codec)
    }
    new EventCodecRegistry(byClass, byTag)

  val empty: EventCodecRegistry = apply()
