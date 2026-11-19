package io.parapet.journal

/** Controls the storage guarantee acknowledged by a successful journal append. */
enum JournalDurability:
  /** File data and metadata are forced before append completion. */
  case HostCrash

  /** Append completes when the operating system has accepted the bytes into its page cache. */
  case ProcessCrash

/** Tuning for [[DeliveryJournal]].
  *
  * @param enabled
  *   whether deliveries are recorded to the journal; off by default.
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
    dataDir: String = "parapet-journal",
    batchSize: Int = JournalConfig.DefaultBatchSize,
    writeMode: Recorder.WriteMode = Recorder.WriteMode.Buffered,
    durability: JournalDurability = JournalDurability.HostCrash,
    maxSegmentBytes: Long = JournalStoreLocal.DefaultMaxSegmentBytes,
    maxEntryBytes: Int = JournalStoreLocal.DefaultMaxEntryBytes
)

object JournalConfig:
  val DefaultBatchSize: Int = 1024

  val default: JournalConfig = JournalConfig()
