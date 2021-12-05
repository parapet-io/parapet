package io.parapet

import java.util.UUID

case class ProcessRef(private[parapet] val value: String) {
  override def toString: String = value
}

object ProcessRef {
  val ParapetPrefix = "parapet"
  val SystemRef: ProcessRef = ProcessRef(ParapetPrefix + "-system")
  val DeadLetterRef: ProcessRef = ProcessRef(ParapetPrefix + "-deadletter")
  val SchedulerRef: ProcessRef = ProcessRef(ParapetPrefix + "-scheduler")
  val UndefinedRef: ProcessRef = ProcessRef(ParapetPrefix + "-undefined")
  val NoopRef: ProcessRef = ProcessRef(ParapetPrefix + "-noop")

  def apply(): ProcessRef = jdkUUIDRef

  def jdkUUIDRef: ProcessRef = new ProcessRef(UUID.randomUUID().toString)
}
