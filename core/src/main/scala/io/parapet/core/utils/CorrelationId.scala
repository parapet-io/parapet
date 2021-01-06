package io.parapet.core.utils

import java.util.UUID

case class CorrelationId(value: String) {
  override def toString: String = value
}

object CorrelationId {
  def apply(): CorrelationId = CorrelationId(UUID.randomUUID().toString)
}