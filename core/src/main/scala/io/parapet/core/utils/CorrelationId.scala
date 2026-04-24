package io.parapet.core.utils

import java.util.UUID

/** A short identifier used to correlate related log lines, events, and external requests across process boundaries.
  *
  * Distinct from [[io.parapet.core.ExecutionTrace]] which records causal *paths*; a `CorrelationId` is just a single id
  * intended for tagging.
  *
  * @param value
  *   the underlying string id (typically a UUID).
  */
case class CorrelationId(value: String) {
  override def toString: String = value
}

/** [[CorrelationId]] factories. */
object CorrelationId {

  /** Allocates a fresh UUID-backed [[CorrelationId]]. */
  def apply(): CorrelationId = CorrelationId(UUID.randomUUID().toString)
}
