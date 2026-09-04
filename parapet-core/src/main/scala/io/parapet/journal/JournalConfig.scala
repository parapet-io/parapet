package io.parapet.journal

/** Controls when admitting a delivery waits for durable journal storage. */
enum JournalWriteMode:
  /** Admissions may return while their delivery remains buffered. */
  case Buffered

  /** Every admission returns only after its delivery and all earlier admissions are durable. */
  case WriteAhead

/** Tuning for [[DeliveryRecorder]].
  *
  * @param enabled
  *   whether deliveries are recorded to the journal; off by default.
  * @param requireCodec
  *   how to treat a delivery whose event has no registered codec: `false` (default) skips it (not journaled); `true`
  *   fails loud, requiring every event to be encodable.
  * @param dataDir
  *   directory holding the journal segment files.
  * @param batchSize
  *   maximum entries in a buffered batch.
  * @param writeMode
  *   durability guarantee applied to each delivery admission.
  * @param maxSegmentBytes
  *   target size at which the active journal file is sealed and rotated.
  * @param maxEntryBytes
  *   maximum encoded size accepted for one delivery.
  */
final case class JournalConfig(
    enabled: Boolean = false,
    requireCodec: Boolean = false,
    dataDir: String = "parapet-journal",
    batchSize: Int = JournalConfig.DefaultBatchSize,
    writeMode: JournalWriteMode = JournalWriteMode.Buffered,
    maxSegmentBytes: Long = JournalStoreLocal.DefaultMaxSegmentBytes,
    maxEntryBytes: Int = JournalStoreLocal.DefaultMaxEntryBytes
)

object JournalConfig:
  val DefaultBatchSize: Int = 1024

  val default: JournalConfig = JournalConfig()
