package io.parapet.core.journal

import io.parapet.ProcessRef

/** Recorded delivery.
  *
  * @param seq
  *   global delivery position.
  * @param id
  *   identity of the delivered envelope.
  * @param sender
  *   the originating process.
  * @param receiver
  *   the addressed process.
  * @param cause
  *   id of the envelope that caused this delivery (`0` for none), preserving runtime lineage.
  * @param event
  *   the encoded event payload.
  * @param tag
  *   codec tag identifying how [[event]] was encoded.
  * @param schemaVersion
  *   version of that codec's layout.
  */
final case class JournalEntry(
    seq: Long,
    id: Long,
    sender: ProcessRef.Unknown,
    receiver: ProcessRef.Unknown,
    cause: Long,
    event: Array[Byte],
    tag: String,
    schemaVersion: Int
)
