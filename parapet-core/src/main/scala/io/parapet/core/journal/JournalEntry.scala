package io.parapet.core.journal

import io.parapet.ProcessRef

/** One recorded delivery: the event handed to `receiver` at global delivery position `seq`.
  *
  * @param seq
  *   global delivery position
  * @param sender
  *   the originating process, so the re-delivered envelope carries its true sender.
  * @param receiver
  *   the addressed process
  * @param cause
  *   id of the envelope that caused this delivery (`0` for none), preserving lineage.
  * @param event
  *   the encoded event payload.
  */
final case class JournalEntry(
    seq: Long,
    sender: ProcessRef.Unknown,
    receiver: ProcessRef.Unknown,
    cause: Long,
    event: Array[Byte]
)
