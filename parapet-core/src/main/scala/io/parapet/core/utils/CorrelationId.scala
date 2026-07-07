package io.parapet.core.utils

import java.util.UUID

/** A short identifier used to correlate related log lines, events, and external requests across process boundaries.
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
