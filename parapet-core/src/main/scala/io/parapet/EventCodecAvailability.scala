package io.parapet

import scala.util.NotGiven

/** Evidence that an [[EventCodec]] is either available or absent for the input protocol `A`.
  *
  * Ordinary processes may omit a codec; replayable processes require one when recovery is enabled.
  */
sealed trait EventCodecAvailability[A <: Event] {

  /** Returns the available codec, or `None` when the protocol has no codec. */
  def toOption: Option[EventCodec[A]]

}

object EventCodecAvailability {

  /** Codec evidence for an encodable protocol. */
  final case class Available[A <: Event](codec: EventCodec[A]) extends EventCodecAvailability[A] {
    override def toOption: Option[EventCodec[A]] = Some(codec)
  }

  /** Codec evidence for a protocol without an available codec. */
  final case class Missing[A <: Event]() extends EventCodecAvailability[A] {
    override def toOption: Option[EventCodec[A]] = Option.empty
  }

  given available[A <: Event](using codec: EventCodec[A]): EventCodecAvailability[A] = Available(codec)

  given missing[A <: Event](using NotGiven[EventCodec[A]]): EventCodecAvailability[A] = Missing[A]()

}
