package io.parapet.journal

/** Controls when admitting a delivery waits for durable journal storage. */
enum JournalWriteMode:
  /** Admissions may return while their delivery remains buffered. */
  case Buffered

  /** Every admission waits until its delivery and all earlier admissions satisfy the configured storage guarantee. */
  case WriteAhead

/** Controls the storage guarantee acknowledged by a successful journal append. */
enum JournalDurability:
  /** File data and metadata are forced before append completion. */
  case HostCrash

  /** Append completes when the operating system has accepted the bytes into its page cache. */
  case ProcessCrash

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
  *   whether delivery admission waits for the configured storage guarantee.
  * @param durability
  *   storage guarantee acknowledged by each journal append.
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
    durability: JournalDurability = JournalDurability.HostCrash,
    maxSegmentBytes: Long = JournalStoreLocal.DefaultMaxSegmentBytes,
    maxEntryBytes: Int = JournalStoreLocal.DefaultMaxEntryBytes
)

object JournalConfig:
  val DefaultBatchSize: Int = 1024

  val default: JournalConfig = JournalConfig()
