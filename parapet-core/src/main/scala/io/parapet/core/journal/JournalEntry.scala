package io.parapet.core.journal

import io.parapet.ProcessRef

/** One recorded delivery: the event handed to `receiver` at global delivery position `seq`.
  *
  * @param seq
  *   global delivery position
  * @param id
  *   identity of the delivered envelope, so the delivery can be re-created with its original id and lineage referring
  *   to it (via [[cause]]) stays resolvable across a restart.
  * @param sender
  *   the originating process, so the re-delivered envelope carries its true sender.
  * @param receiver
  *   the addressed process
  * @param cause
  *   id of the envelope that caused this delivery (`0` for none), preserving runtime lineage.
  * @param event
  *   the encoded event payload.
  */
final case class JournalEntry(
    seq: Long,
    id: Long,
    sender: ProcessRef.Unknown,
    receiver: ProcessRef.Unknown,
    cause: Long,
    event: Array[Byte]
)
