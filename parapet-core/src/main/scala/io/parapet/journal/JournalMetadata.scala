package io.parapet.journal

/** Summary of one journal segment.
  *
  * @param minSeq
  *   the lowest delivery `seq` in the segment
  * @param maxSeq
  *   the highest delivery `seq` in the segment
  * @param entryCount
  *   the number of entries
  * @param maxEnvelopeId
  *   the highest envelope id the segment refers to
  * @param createdAt
  *   when the segment was sealed, epoch millis
  */
final case class JournalMetadata(
    minSeq: Long,
    maxSeq: Long,
    entryCount: Int,
    maxEnvelopeId: Long,
    createdAt: Long
)

object JournalMetadata:

  /** Derives metadata from entries. */
  def of(entries: Seq[JournalEntry], createdAt: Long): JournalMetadata =
    require(entries.nonEmpty, "cannot summarize an empty batch")
    var minSeq        = Long.MaxValue
    var maxSeq        = Long.MinValue
    var maxEnvelopeId = 0L
    entries.foreach { e =>
      if e.seq < minSeq then minSeq = e.seq
      if e.seq > maxSeq then maxSeq = e.seq
      if e.id > maxEnvelopeId then maxEnvelopeId = e.id
      if e.cause > maxEnvelopeId then maxEnvelopeId = e.cause
    }
    JournalMetadata(minSeq, maxSeq, entries.size, maxEnvelopeId, createdAt)
