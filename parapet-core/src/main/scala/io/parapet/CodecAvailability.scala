package io.parapet

import scala.util.NotGiven

/** Evidence that a [[Codec]] is either available or absent for the value type `A`.
  *
  * Ordinary processes may omit a codec; replayable processes require one when recovery is enabled.
  */
sealed trait CodecAvailability[A] {

  /** Returns the available codec, or `None` when the type has no codec. */
  def toOption: Option[Codec[A]]

}

object CodecAvailability {

  /** Codec evidence for an encodable protocol. */
  final case class Available[A](codec: Codec[A]) extends CodecAvailability[A] {
    override def toOption: Option[Codec[A]] = Some(codec)
  }

  /** Codec evidence for a protocol without an available codec. */
  final case class Missing[A]() extends CodecAvailability[A] {
    override def toOption: Option[Codec[A]] = Option.empty
  }

  given available[A](using codec: Codec[A]): CodecAvailability[A] = Available(codec)

  given missing[A](using NotGiven[Codec[A]]): CodecAvailability[A] = Missing[A]()

}
