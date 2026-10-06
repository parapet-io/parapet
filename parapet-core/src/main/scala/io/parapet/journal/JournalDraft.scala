package io.parapet.journal

import io.parapet.{Event, EventCodec, ProcessRef}

/** A delivery to record.
  *
  * @param id
  *   identity of the source envelope
  * @param sender
  *   the originating process
  * @param receiver
  *   the addressed process
  * @param cause
  *   id of the envelope that caused this delivery (`0` for none)
  * @param event
  *   the event delivered
  * @param codec
  *   codec for the receiver's input protocol
  */
final case class JournalDraft[A <: Event](
    id: Long,
    sender: ProcessRef.Unknown,
    receiver: ProcessRef.Unknown,
    cause: Long,
    event: A,
    codec: EventCodec[A]
)
