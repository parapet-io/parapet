package io.parapet.journal

/** Durable store for the delivery journal. */
trait JournalStore[F[_]]:

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

  /** Drops every segment whose entries are all `<= upToSeq` (dead once covered by snapshots). Segments straddling
    * `upToSeq` are kept whole. Callers recording through [[DeliveryRecorder]] use [[DeliveryRecorder.truncate]] so
    * buffered admissions are published first.
    */
  def truncate(upToSeq: Long): F[Unit]
