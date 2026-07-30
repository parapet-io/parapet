package io.parapet.core.journal

/** Durable store for the delivery journal: an ordered sequence of [[JournalEntry]]s written in batches and read back by
  * seq range. Entry bytes are produced/consumed by [[JournalEntryBinaryFormat]].
  */
trait JournalStore[F[_]]:

  /** Persists `batch` as one segment. Entries are expected in ascending `seq` order; a batch is stored atomically -
    * either all of it is durable or none of it is.
    */
  def append(batch: Seq[JournalEntry]): F[Unit]

  /** All intact entries with `seq > afterSeq`, in ascending `seq` order, merged across segments. */
  def read(afterSeq: Long): F[Vector[JournalEntry]]

  /** The highest `seq` stored, if any - the delivery-sequence high-water for resuming after recovery. */
  def maxSeq: F[Option[Long]]

  /** Drops every segment whose entries are all `<= upToSeq` (dead once covered by snapshots). Segments straddling
    * `upToSeq` are kept whole.
    */
  def truncate(upToSeq: Long): F[Unit]
