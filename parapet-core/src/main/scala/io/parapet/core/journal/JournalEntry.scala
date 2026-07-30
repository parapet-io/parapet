package io.parapet.core.journal

import io.parapet.ProcessRef

/** One recorded delivery: the event handed to `receiver` at global delivery position `seq`.
  *
  * The event payload is stored opaque - encoded by the receiver's [[EventCodec]] - so the journal does not depend on
  * the event types. Replay decodes it with the same codec.
  *
  * @param seq
  *   global delivery position (`Context.seqCounter`), the same ruler as `Snapshot.Metadata.seq`; drives replay ordering
  *   and the per-receiver `seq > snapshot.seq` filter.
  * @param sender
  *   the originating process, so the re-delivered envelope carries its true sender.
  * @param receiver
  *   the addressed process; picks the codec on replay and is filtered by its own snapshot.
  * @param cause
  *   id of the envelope that caused this delivery (`0` for none), preserving lineage.
  * @param event
  *   the codec-encoded event payload.
  */
final case class JournalEntry(
    seq: Long,
    sender: ProcessRef.Unknown,
    receiver: ProcessRef.Unknown,
    cause: Long,
    event: Array[Byte]
)
