package io.parapet.core.journal

import io.parapet.ProcessRef

/** An encoded delivery that has not yet been assigned a global position. The recorder stamps the `seq` at admission, so
  * that sequence allocation and journal insertion share one linearization point.
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
  *   the encoded event payload (already defensively copied by the caller)
  */
final case class JournalDraft(
    id: Long,
    sender: ProcessRef.Unknown,
    receiver: ProcessRef.Unknown,
    cause: Long,
    event: Array[Byte]
):
  def withSeq(seq: Long): JournalEntry = JournalEntry(seq, id, sender, receiver, cause, event)
