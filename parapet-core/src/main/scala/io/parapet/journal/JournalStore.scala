package io.parapet.journal

/** Durable store for the delivery journal. A store is opened once, used, then closed. */
trait JournalStore[F[_]]:

  /** Opens the store. It must be called once, before any other method. */
  def open(): F[Unit]

  /** Appends `entries` in ascending `seq` order above the durable high-water. Successful completion means every entry
    * satisfies the store's configured durability guarantee. An append error is terminal for the writer.
    */
  def append(entries: Vector[JournalEntry]): F[Unit]

  /** All intact entries with `seq > afterSeq`, in ascending `seq` order. */
  def read(afterSeq: Long): F[Vector[JournalEntry]]

  /** The durable delivery-sequence high-water, including discarded entries, if any. */
  def maxSeq: F[Option[Long]]

  /** The durable envelope-id high-water: the greatest entry `id` or `cause` ever recorded, including discarded entries,
    * if any.
    */
  def maxEnvelopeId: F[Option[Long]]

  /** Drops segments whose entries are all `<= upToSeq` (dead once covered by snapshots). Segments straddling `upToSeq`
    * are kept whole, and a store may keep the segment it is still writing.
    */
  def truncate(upToSeq: Long): F[Unit]

  /** Closes the store. */
  def close(): F[Unit]
