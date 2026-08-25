package io.parapet.journal

import io.parapet.{Event, ProcessRef}

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
  */
final case class JournalDraft(
    id: Long,
    sender: ProcessRef.Unknown,
    receiver: ProcessRef.Unknown,
    cause: Long,
    event: Event
)
