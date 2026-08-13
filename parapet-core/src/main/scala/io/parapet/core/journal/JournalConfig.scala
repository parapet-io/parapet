package io.parapet.core.journal

import io.parapet.effect.Backoff

import scala.concurrent.duration.*

/** Tuning for [[DeliveryRecorder]].
  *
  * @param enabled
  *   master switch for recording the delivery journal.
  * @param dataDir
  *   directory holding the journal segment files.
  * @param batchSize
  *   entries buffered before a batch is flushed.
  * @param maxRetries
  *   store retries before a write fails.
  * @param backoff
  *   delay between store retries.
  */
final case class JournalConfig(
    enabled: Boolean = false,
    dataDir: String = "parapet-journal",
    batchSize: Int = JournalConfig.DefaultBatchSize,
    maxRetries: Int = JournalConfig.DefaultMaxRetries,
    backoff: Backoff = JournalConfig.DefaultBackoff
)

object JournalConfig:
  val DefaultBatchSize: Int   = 1024
  val DefaultMaxRetries: Int  = 8
  val DefaultBackoff: Backoff = Backoff.Exp(base = 50.millis, max = 5.seconds)

  val default: JournalConfig = JournalConfig()
