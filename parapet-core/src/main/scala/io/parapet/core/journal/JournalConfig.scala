package io.parapet.core.journal

import io.parapet.effect.Backoff

import scala.concurrent.duration.*

/** Tuning for [[JournalManager]].
  *
  * @param batchSize
  *   entries buffered before a batch is flushed.
  * @param queueCapacity
  *   capacity of the writer's inbound queue.
  * @param maxRetries
  *   store retries before a write fails.
  * @param backoff
  *   delay between store retries.
  */
final case class JournalConfig(
    batchSize: Int = JournalConfig.DefaultBatchSize,
    queueCapacity: Int = JournalConfig.DefaultQueueCapacity,
    maxRetries: Int = JournalConfig.DefaultMaxRetries,
    backoff: Backoff = JournalConfig.DefaultBackoff
)

object JournalConfig:
  val DefaultBatchSize: Int         = 1024
  val DefaultQueueCapacity: Int     = 4096
  val DefaultMaxRetries: Int        = 8
  val DefaultBackoff: Backoff       = Backoff.Exp(base = 50.millis, max = 5.seconds)

  val default: JournalConfig = JournalConfig()
