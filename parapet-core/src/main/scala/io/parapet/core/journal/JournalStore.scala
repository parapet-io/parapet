package io.parapet.core.journal

/** Durable store for the delivery journal: an ordered sequence of [[JournalEntry]]s written in batches and read back by
  * seq range. Entry bytes are produced/consumed by [[JournalEntryBinaryFormat]].
  */
trait JournalStore[F[_]]:

  /** Persists `batch` as one segment. Entries are expected in ascending `seq` order. Each attempt is atomic: it leaves
    * either none or the complete batch durable; an error may be ambiguous between those two outcomes. Because a caller
    * may retry after such an error, appending the same batch again must be idempotent and must not create duplicate
    * logical entries.
    */
  def append(batch: Seq[JournalEntry]): F[Unit]

  /** All intact entries with `seq > afterSeq`, in ascending `seq` order, merged across segments. */
  def read(afterSeq: Long): F[Vector[JournalEntry]]

  /** The highest `seq` stored, if any - the delivery-sequence high-water for resuming after recovery. */
  def maxSeq: F[Option[Long]]

  /** The highest envelope id the journal refers to - the greater of any entry's own `id` and any entry's `cause` - if
    * any.
    */
  def maxEnvelopeId: F[Option[Long]]

  /** Drops every segment whose entries are all `<= upToSeq` (dead once covered by snapshots). Segments straddling
    * `upToSeq` are kept whole.
    */
  def truncate(upToSeq: Long): F[Unit]
